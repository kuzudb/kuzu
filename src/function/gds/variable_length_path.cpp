#include <vector>

#include "binder/expression/expression_util.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "graph/graph.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

class VarLenPathsOutputWriter final : public PathsOutputWriter {
public:
    VarLenPathsOutputWriter(main::ClientContext* context, RJOutputs* rjOutputs,
        processor::NodeOffsetMaskMap* outputNodeMask, PathsOutputWriterInfo info)
        : PathsOutputWriter{context, rjOutputs, outputNodeMask, info} {}

    bool skipInternal(common::nodeID_t dstNodeID) const override {
        auto pathsOutputs = rjOutputs->ptrCast<PathsOutputs>();
        auto firstParent = pathsOutputs->bfsGraph.getCurFixedParentPtrs()[dstNodeID.offset].load(
            std::memory_order_relaxed);
        // For variable lengths joins, we skip a destination node d in the following conditions:
        //    (i) if no path has reached d from the source, except when the lower bound is 0.
        //    (ii) the longest path that has reached d, which is stored in the iter value of the
        //    firstParent is smaller than the lower bound.
        // We also do not output any results if a destination node has not been reached.
        if (nullptr == firstParent) {
            return info.lowerBound > 0;
        } else {
            return firstParent->getIter() < info.lowerBound;
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<VarLenPathsOutputWriter>(context, rjOutputs, outputNodeMask, info);
    }
};

struct VarLenJoinsEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* frontierPair;
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* parentPtrsBlock = nullptr;

    VarLenJoinsEdgeCompute(DoublePathLengthsFrontierPair* frontierPair, BFSGraph* bfsGraph)
        : frontierPair{frontierPair}, bfsGraph{bfsGraph} {
        parentPtrsBlock = bfsGraph->addNewBlock();
    };

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::GraphScanState::Chunk& chunk,
        bool isFwd) override {
        std::vector<nodeID_t> activeNodes;
        chunk.forEach([&](auto nbrNode, auto edgeID) {
            // We should always update the nbrID in variable length joins
            if (!parentPtrsBlock->hasSpace()) {
                parentPtrsBlock = bfsGraph->addNewBlock();
            }
            bfsGraph->addParent(frontierPair->getCurrentIter(), parentPtrsBlock,
                nbrNode /* child */, boundNodeID /* parent */, edgeID, isFwd);

            activeNodes.push_back(nbrNode);
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<VarLenJoinsEdgeCompute>(frontierPair, bfsGraph);
    }
};

/**
 * Algorithm for parallel all shortest paths computation, so all shortest paths from a source to
 * is returned for each destination. If paths are not returned, multiplicities indicate the
 * number of paths to each destination.
 */
class VarLenJoinsAlgorithm final : public RJAlgorithm {
public:
    VarLenJoinsAlgorithm() = default;
    VarLenJoinsAlgorithm(const VarLenJoinsAlgorithm& other) : RJAlgorithm(other) {}

    /*
     * Inputs include the following:
     *
     * graph::ANY
     * srcNode::NODE
     * lowerBound::INT64
     * upperBound::INT64
     * direction::STRING
     */
    std::vector<LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64, LogicalTypeID::INT64,
            LogicalTypeID::STRING};
    }

    void bind(const expression_vector& params, Binder* binder,
        graph::GraphEntry& graphEntry) override {
        auto nodeInput = params[1];
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        auto lowerBound = ExpressionUtil::getLiteralValue<int64_t>(*params[2]);
        auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*params[3]);
        validateLowerUpperBound(lowerBound, upperBound);
        auto extendDirection = ExtendDirectionUtil::fromString(
            ExpressionUtil::getLiteralValue<std::string>(*params[4]));
        bindData = std::make_unique<RJBindData>(nodeInput, nodeOutput, lowerBound, upperBound,
            PathSemantic::WALK, extendDirection);
        bindColumnExpressions(binder);
    }

    binder::expression_vector getResultColumns(Binder*) const override {
        auto columns = getBaseResultColumns();
        auto rjBindData = bindData->ptrCast<RJBindData>();
        if (rjBindData->writePath) {
            columns.push_back(rjBindData->pathNodeIDsExpr);
            columns.push_back(rjBindData->pathEdgeIDsExpr);
        }
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<VarLenJoinsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto mm = clientContext->getMemoryManager();
        auto nodeTableToNumNodes = sharedState->graph->getNumNodesMap(clientContext->getTx());
        auto output = std::make_unique<PathsOutputs>(nodeTableToNumNodes, sourceNodeID, mm);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<VarLenPathsOutputWriter>(clientContext, output.get(),
            sharedState->getOutputNodeMaskMap(), std::move(writerInfo));
        auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(nodeTableToNumNodes,
            clientContext->getMaxNumThreadForExec(), mm);
        auto edgeCompute =
            std::make_unique<VarLenJoinsEdgeCompute>(frontierPair.get(), &output->bfsGraph);
        return RJCompState(std::move(frontierPair), std::move(edgeCompute), std::move(output),
            std::move(outputWriter));
    }
};

function_set VarLenJoinsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction VarLenJoinsFunction::getFunction() {
    auto algo = std::make_unique<VarLenJoinsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

} // namespace function
} // namespace kuzu
