#include "binder/expression/node_expression.h"
#include "function/gds/auxiliary_state/path_auxiliary_state.h"
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
    VarLenPathsOutputWriter(main::ClientContext* context, common::NodeOffsetMaskMap* outputNodeMask,
        common::nodeID_t sourceNodeID, PathsOutputWriterInfo info, BFSGraph& bfsGraph)
        : PathsOutputWriter{context, outputNodeMask, sourceNodeID, info, bfsGraph} {}

    bool skipInternal(common::nodeID_t dstNodeID) const override {
        auto head = bfsGraph.getParentListHead(dstNodeID.offset);
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
        return std::make_unique<VarLenPathsOutputWriter>(context, outputNodeMask, sourceNodeID,
            info, bfsGraph);
    }
};

struct VarLenJoinsEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* frontierPair;
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* block = nullptr;

    VarLenJoinsEdgeCompute(DoublePathLengthsFrontierPair* frontierPair, BFSGraph* bfsGraph)
        : frontierPair{frontierPair}, bfsGraph{bfsGraph} {
        block = bfsGraph->addNewBlock();
    };

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool fwdEdge) override {
        std::vector<nodeID_t> activeNodes;
        chunk.forEach([&](auto nbrNodeID, auto edgeID) {
            // We should always update the nbrID in variable length joins
            if (!block->hasSpace()) {
                block = bfsGraph->addNewBlock();
            }
            bfsGraph->addParent(frontierPair->getCurrentIter(), boundNodeID, edgeID, nbrNodeID,
                fwdEdge, block);
            activeNodes.push_back(nbrNodeID);
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
    std::string getFunctionName() const override { return VarLenJoinsFunction::name; }

    // return srcNodeID, dstNodeID, length, [direction, pathNodeIDs, pathEdgeIDs] (if track path)
    binder::expression_vector getResultColumns(const RJBindData& bindData) const override {
        expression_vector columns;
        columns.push_back(bindData.nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.nodeOutput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.lengthExpr);
        if (bindData.writePath) {
            if (bindData.extendDirection == ExtendDirection::BOTH) {
                columns.push_back(bindData.directionExpr);
            }
            columns.push_back(bindData.pathNodeIDsExpr);
            columns.push_back(bindData.pathEdgeIDsExpr);
        }
        return columns;
    }

    std::unique_ptr<RJAlgorithm> copy() const override {
        return std::make_unique<VarLenJoinsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID,
        const RJBindData& bindData, processor::RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto frontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto bfsGraph = getBFSGraph(context, sharedState->graph.get());
        auto writerInfo = bindData.getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<VarLenPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        auto currentFrontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto nextFrontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto frontierPair =
            std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
        auto edgeCompute =
            std::make_unique<VarLenJoinsEdgeCompute>(frontierPair.get(), bfsGraph.get());
        auto auxiliaryState = std::make_unique<PathAuxiliaryState>(std::move(bfsGraph));
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

std::unique_ptr<RJAlgorithm> VarLenJoinsFunction::getAlgorithm() {
    return std::make_unique<VarLenJoinsAlgorithm>();
}

} // namespace function
} // namespace kuzu
