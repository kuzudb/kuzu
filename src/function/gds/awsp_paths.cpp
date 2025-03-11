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
    AWSPPathsOutputWriter(main::ClientContext* context, common::NodeOffsetMaskMap* outputNodeMask,
        common::nodeID_t sourceNodeID, PathsOutputWriterInfo info, BFSGraph& bfsGraph)
        : PathsOutputWriter{context, outputNodeMask, sourceNodeID, info, bfsGraph} {
        costVector = createVector(LogicalType::DOUBLE());
    }

    void write(processor::FactorizedTable& fTable, common::nodeID_t dstNodeID,
        LimitCounter* counter) override {
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
    std::string getFunctionName() const override { return AllWeightedSPPathsFunction::name; }

    // return srcNodeID, dstNodeID, length, [direction], pathNodeIDs, pathEdgeIDs, weight
    expression_vector getResultColumns(const RJBindData& bindData) const override {
        expression_vector columns;
        columns.push_back(bindData.nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.nodeOutput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.lengthExpr);
        if (bindData.extendDirection == ExtendDirection::BOTH) {
            columns.push_back(bindData.directionExpr);
        }
        columns.push_back(bindData.pathNodeIDsExpr);
        columns.push_back(bindData.pathEdgeIDsExpr);
        columns.push_back(bindData.weightOutputExpr);
        return columns;
    }

    std::unique_ptr<RJAlgorithm> copy() const override {
        return std::make_unique<AllWeightedSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(processor::ExecutionContext* context, nodeID_t sourceNodeID,
        const RJBindData& bindData, processor::RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto graph = sharedState->graph.get();
        auto curFrontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto nextFrontier = PathLengths::getUnvisitedFrontier(context, graph);
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(curFrontier, nextFrontier);
        auto bfsGraph = getBFSGraph(context, graph);
        auto writerInfo = bindData.getPathWriterInfo();
        auto outputWriter = std::make_unique<AWSPPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        std::unique_ptr<GDSComputeState> gdsState;
        visit(bindData.weightPropertyExpr->getDataType(), [&]<typename T>(T) {
            auto edgeCompute = std::make_unique<AWSPPathsEdgeCompute<T>>(*bfsGraph);
            auto auxiliaryState = std::make_unique<WSPPathsAuxiliaryState>(std::move(bfsGraph));
            gdsState =
                std::make_unique<GDSComputeState>(std::move(frontierPair), std::move(edgeCompute),
                    std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        });
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

std::unique_ptr<RJAlgorithm> AllWeightedSPPathsFunction::getAlgorithm() {
    return std::make_unique<AllWeightedSPPathsAlgorithm>();
}

} // namespace function
} // namespace kuzu
