#include "binder/expression/node_expression.h"
#include "function/gds/auxiliary_state/path_auxiliary_state.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

class ASPPathsEdgeCompute : public SPEdgeCompute {
public:
    ASPPathsEdgeCompute(SinglePathLengthsFrontierPair* frontiersPair, BFSGraph* bfsGraph)
        : SPEdgeCompute{frontiersPair}, bfsGraph{bfsGraph} {
        block = bfsGraph->addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& resultChunk,
        bool fwdEdge) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto edgeID) {
            auto nbrLen =
                frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNodeID.offset);
            // We should update in 2 cases: 1) if nbrID is being visited
            // for the first time, i.e., when its value in the pathLengths frontier is
            // PathLengths::UNVISITED. Or 2) if nbrID has already been visited but in this
            // iteration, so it's value is curIter + 1.
            auto shouldUpdate =
                nbrLen == PathLengths::UNVISITED || nbrLen == frontierPair->getCurrentIter();
            if (shouldUpdate) {
                if (!block->hasSpace()) {
                    block = bfsGraph->addNewBlock();
                }
                bfsGraph->addParent(frontierPair->getCurrentIter(), boundNodeID, edgeID, nbrNodeID,
                    fwdEdge, block);
            }
            if (nbrLen == PathLengths::UNVISITED) {
                activeNodes.push_back(nbrNodeID);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<ASPPathsEdgeCompute>(frontierPair, bfsGraph);
    }

private:
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* block = nullptr;
};

class AllSPPathsAlgorithm final : public RJAlgorithm {
public:
    std::string getFunctionName() const override { return AllSPPathsFunction::name; }

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
        return columns;
    }

    std::unique_ptr<RJAlgorithm> copy() const override {
        return std::make_unique<AllSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID,
        const RJBindData& bindData, processor::RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto frontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto bfsGraph = getBFSGraph(context, sharedState->graph.get());
        auto writerInfo = bindData.getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<ASPPathsEdgeCompute>(frontierPair.get(), bfsGraph.get());
        auto auxiliaryState = std::make_unique<PathAuxiliaryState>(std::move(bfsGraph));
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

std::unique_ptr<RJAlgorithm> AllSPPathsFunction::getAlgorithm() {
    return std::make_unique<AllSPPathsAlgorithm>();
}

} // namespace function
} // namespace kuzu
