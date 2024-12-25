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
        auto head = pathsOutputs->bfsGraph->getParentListHead(dstNodeID.offset);
        // For variable lengths joins, we skip a destination node d in the following conditions:
        //    (i) if no path has reached d from the source, except when the lower bound is 0.
        //    (ii) the longest path that has reached d, which is stored in the iter value of the
        //    firstParent is smaller than the lower bound.
        // We also do not output any results if a destination node has not been reached.
        if (nullptr == head) {
            return info.lowerBound > 0;
        } else {
            return head->getIter() < info.lowerBound;
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

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
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

    void bind(const GDSBindInput& input, main::ClientContext& context) override {
        auto graphName = ExpressionUtil::getLiteralValue<std::string>(*input.getParam(0));
        auto graphEntry = bindGraphEntry(context, graphName);
        auto nodeOutput = bindNodeOutput(input.binder, graphEntry.nodeEntries);
        auto rjBindData = std::make_unique<RJBindData>(std::move(graphEntry), nodeOutput);
        rjBindData->nodeInput = input.getParam(1);
        auto lowerBound = ExpressionUtil::getLiteralValue<int64_t>(*input.getParam(2));
        auto upperBound = ExpressionUtil::getLiteralValue<int64_t>(*input.getParam(3));
        validateLowerUpperBound(lowerBound, upperBound);
        rjBindData->lowerBound = lowerBound;
        rjBindData->upperBound = upperBound;
        rjBindData->semantic = PathSemantic::WALK;
        rjBindData->extendDirection = ExtendDirectionUtil::fromString(
            ExpressionUtil::getLiteralValue<std::string>(*input.getParam(4)));
        bindData = std::move(rjBindData);
        bindColumnExpressions(input.binder);
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
        auto frontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto bfsGraph = getBFSGraph(context);
        auto output = std::make_unique<PathsOutputs>(sourceNodeID, frontier, std::move(bfsGraph));
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<VarLenPathsOutputWriter>(clientContext, output.get(),
            sharedState->getOutputNodeMaskMap(), std::move(writerInfo));
        auto currentFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto nextFrontier = getPathLengthsFrontier(context, PathLengths::UNVISITED);
        auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier,
            nextFrontier, clientContext->getMaxNumThreadForExec());
        auto edgeCompute =
            std::make_unique<VarLenJoinsEdgeCompute>(frontierPair.get(), output->bfsGraph.get());
        return RJCompState(std::move(frontierPair), std::move(edgeCompute),
            sharedState->getOutputNodeMaskMap(), std::move(output), std::move(outputWriter));
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
