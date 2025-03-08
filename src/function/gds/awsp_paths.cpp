#include "binder/expression/node_expression.h"
#include "common/exception/interrupt.h"
#include "function/gds/auxiliary_state/path_auxiliary_state.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "wsp_utils.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename T>
class AWSPPathsEdgeCompute : public EdgeCompute {
public:
    explicit AWSPPathsEdgeCompute(BFSGraph& bfsGraph) : bfsGraph{bfsGraph} {
        block = bfsGraph.addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool fwdEdge) override {
        std::vector<nodeID_t> result;
        chunk.forEach<T>([&](auto nbrNodeID, auto edgeID, auto weight) {
            if (!block->hasSpace()) {
                block = bfsGraph.addNewBlock();
            }
            if (bfsGraph.tryAddParentWithWeight(boundNodeID, edgeID, nbrNodeID, fwdEdge,
                    (double)weight, block)) {
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<AWSPPathsEdgeCompute<T>>(bfsGraph);
    }

private:
    BFSGraph& bfsGraph;
    ObjectBlock<ParentList>* block = nullptr;
};

class AWSPPathsOutputWriter : public PathsOutputWriter {
public:
    AWSPPathsOutputWriter(main::ClientContext* context,
        processor::NodeOffsetMaskMap* outputNodeMask, common::nodeID_t sourceNodeID,
        PathsOutputWriterInfo info, BFSGraph& bfsGraph)
        : PathsOutputWriter{context, outputNodeMask, sourceNodeID, info, bfsGraph} {
        costVector = createVector(LogicalType::DOUBLE(), context->getMemoryManager());
    }

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        processor::GDSOutputCounter* counter) override {
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        auto firstParent = bfsGraph.getParentListHead(dstNodeID.offset);
        KU_ASSERT(firstParent != nullptr);
        costVector->setValue<double>(0, firstParent->getCost());
        std::vector<ParentList*> curPath;
        curPath.push_back(firstParent);
        auto backtracking = false;
        while (!curPath.empty()) {
            if (context->interrupted()) {
                throw InterruptException{};
            }
            if (curPath[curPath.size() - 1]->getCost() == 0) { // Find source. Start writing path.
                curPath.pop_back();                            // avoid writing source node.
                writePath(curPath);
                fTable.append(vectors);
                if (updateCounterAndTerminate(counter)) {
                    return;
                }
                backtracking = true;
            }
            auto topIdx = curPath.size() - 1;
            if (backtracking) {
                auto next = curPath[topIdx]->getNextPtr();
                if (next != nullptr) { // Find next top node with the same cost.
                    KU_ASSERT(curPath[topIdx]->getCost() == next->getCost());
                    curPath[topIdx] = next;
                    backtracking = false;
                } else { // Move to next top.
                    curPath.pop_back();
                }
            } else { // Forward track fill path.
                auto parent = bfsGraph.getParentListHead(curPath[topIdx]->getNodeID());
                KU_ASSERT(parent != nullptr);
                curPath.push_back(parent);
                backtracking = false;
            }
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<AWSPPathsOutputWriter>(context, outputNodeMask, sourceNodeID, info,
            bfsGraph);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        if (dstNodeID == sourceNodeID) {
            return true;
        }
        auto parent = bfsGraph.getParentListHead(dstNodeID.offset);
        return parent == nullptr || parent->getCost() == std::numeric_limits<double>::max();
    }

private:
    std::unique_ptr<ValueVector> costVector;
};

class AllWeightedSPPathsAlgorithm : public RJAlgorithm {
public:
    AllWeightedSPPathsAlgorithm() = default;
    AllWeightedSPPathsAlgorithm(const AllWeightedSPPathsAlgorithm& other) : RJAlgorithm{other} {}

    // return srcNodeID, dstNodeID, length, [direction], pathNodeIDs, pathEdgeIDs, weight
    expression_vector getResultColumns(const GDSBindInput&) const override {
        auto rjBindData = bindData->ptrCast<RJBindData>();
        expression_vector columns;
        columns.push_back(bindData->getNodeInput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData->getNodeOutput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(rjBindData->lengthExpr);
        if (rjBindData->extendDirection == ExtendDirection::BOTH) {
            columns.push_back(rjBindData->directionExpr);
        }
        columns.push_back(rjBindData->pathNodeIDsExpr);
        columns.push_back(rjBindData->pathEdgeIDsExpr);
        columns.push_back(rjBindData->weightOutputExpr);
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<AllWeightedSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(processor::ExecutionContext* context,
        nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto curFrontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto nextFrontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(curFrontier, nextFrontier);
        auto bfsGraph = getBFSGraph(context);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        auto outputWriter = std::make_unique<AWSPPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(rjBindData->weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<AWSPPathsEdgeCompute<T>>(*bfsGraph);
            auto auxiliaryState = std::make_unique<WSPPathsAuxiliaryState>(std::move(bfsGraph));
            gdsState =
                std::make_unique<GDSComputeState>(std::move(frontierPair), std::move(edgeCompute),
                    std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        });
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

function_set AllWeightedSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction AllWeightedSPPathsFunction::getFunction() {
    auto algo = std::make_unique<AllWeightedSPPathsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

} // namespace function
} // namespace kuzu
