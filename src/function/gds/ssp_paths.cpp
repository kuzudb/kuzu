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

class SSPPathsEdgeCompute : public SPEdgeCompute {
public:
    SSPPathsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair, BFSGraph* bfsGraph)
        : SPEdgeCompute{frontierPair}, bfsGraph{bfsGraph} {
        block = bfsGraph->addNewBlock();
    }

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& resultChunk,
        bool isFwd) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNodeID, auto edgeID) {
            if (frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNodeID.offset) ==
                PathLengths::UNVISITED) {
                if (!block->hasSpace()) {
                    block = bfsGraph->addNewBlock();
                }
                bfsGraph->addSingleParent(frontierPair->getCurrentIter(), boundNodeID, edgeID,
                    nbrNodeID, isFwd, block);
                activeNodes.push_back(nbrNodeID);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SSPPathsEdgeCompute>(frontierPair, bfsGraph);
    }

private:
    BFSGraph* bfsGraph;
    ObjectBlock<ParentList>* block = nullptr;
};

class SingleSPPathsAlgorithm : public RJAlgorithm {
public:
    SingleSPPathsAlgorithm() = default;
    SingleSPPathsAlgorithm(const SingleSPPathsAlgorithm& other) : RJAlgorithm{other} {}

    expression_vector getResultColumns(const function::GDSBindInput& /*bindInput*/) const override {
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
        return columns;
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<SingleSPPathsAlgorithm>(*this);
    }

private:
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID) override {
        auto clientContext = context->clientContext;
        auto frontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto bfsGraph = getBFSGraph(context);
        auto rjBindData = bindData->ptrCast<RJBindData>();
        auto writerInfo = rjBindData->getPathWriterInfo();
        writerInfo.pathNodeMask = sharedState->getPathNodeMaskMap();
        auto outputWriter = std::make_unique<SPPathsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, writerInfo, *bfsGraph);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute =
            std::make_unique<SSPPathsEdgeCompute>(frontierPair.get(), bfsGraph.get());
        auto auxiliaryState = std::make_unique<PathAuxiliaryState>(std::move(bfsGraph));
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

function_set SingleSPPathsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<GDSFunction>(getFunction()));
    return result;
}

GDSFunction SingleSPPathsFunction::getFunction() {
    auto algo = std::make_unique<SingleSPPathsAlgorithm>();
    auto params = algo->getParameterTypeIDs();
    return GDSFunction(name, std::move(params), std::move(algo));
}

} // namespace function
} // namespace kuzu
