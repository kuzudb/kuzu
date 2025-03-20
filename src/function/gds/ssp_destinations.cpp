#include <unistd.h>

#include "binder/expression/node_expression.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::graph;
using namespace kuzu::main;

namespace kuzu {
namespace function {

class SSPDestinationsOutputWriter : public RJOutputWriter {
public:
    SSPDestinationsOutputWriter(ClientContext* context, NodeOffsetMaskMap* outputNodeMask,
        nodeID_t sourceNodeID, Frontier* frontier) : RJOutputWriter{context, outputNodeMask, sourceNodeID}, frontier{frontier} {
        lengthVector = createVector(LogicalType::UINT16());
    }

    void beginWriting(table_id_t tableID) override {
        frontier->pinTableID(tableID);
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, LimitCounter* counter) override {
        auto iter = frontier->getIteration(dstNodeID.offset);
        if (iter == FRONTIER_UNVISITED) {
            return;
        }
        writeInternal(iter, fTable, dstNodeID, counter);
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SSPDestinationsOutputWriter>(context, outputNodeMask, sourceNodeID, frontier);
    }

private:
    void writeInternal(iteration_t length, FactorizedTable& fTable, nodeID_t dstNodeID, LimitCounter* counter) {
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, length);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

private:
    Frontier* frontier;
    std::unique_ptr<ValueVector> lengthVector;
};

class SSPDestinationsEdgeCompute : public SPEdgeCompute {
public:
    explicit SSPDestinationsEdgeCompute(SPFrontierPair* frontierPair)
        : SPEdgeCompute{frontierPair} {};

    std::vector<nodeID_t> edgeCompute(nodeID_t, NbrScanState::Chunk& resultChunk,
        bool) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNode, auto) {
            auto iter = frontierPair->getNextFrontierValue(nbrNode.offset);
            if (iter == FRONTIER_UNVISITED) {
                activeNodes.push_back(nbrNode);
            }
        });
        return activeNodes;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<SSPDestinationsEdgeCompute>(frontierPair);
    }
};

/**
 * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
 * one arbitrary shortest path is returned for each destination. If paths are not returned,
 * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
 * d is returned only once).
 */
class SingleSPDestinationsAlgorithm : public RJAlgorithm {
public:
    std::string getFunctionName() const override { return SingleSPDestinationsFunction::name; }

    expression_vector getResultColumns(const RJBindData& bindData) const override {
        expression_vector columns;
        columns.push_back(bindData.nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.nodeOutput->constCast<NodeExpression>().getInternalID());
        columns.push_back(bindData.lengthExpr);
        return columns;
    }

    std::unique_ptr<RJAlgorithm> copy() const override {
        return std::make_unique<SingleSPDestinationsAlgorithm>(*this);
    }

private:
    std::unique_ptr<GDSComputeState> getComputeState(ExecutionContext* context, const RJBindData& bindData, RecursiveExtendSharedState* sharedState) override {
        auto denseFrontier = DenseFrontier::getUninitializedFrontier(context, sharedState->graph.get());
        auto frontierPair = std::make_unique<SPFrontierPair>(denseFrontier);
        auto edgeCompute = std::make_unique<SSPDestinationsEdgeCompute>(frontierPair.get());
        auto auxiliaryState = std::make_unique<EmptyGDSAuxiliaryState>();
        return std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState));
    }

    std::unique_ptr<RJOutputWriter> getOutputWriter(ExecutionContext* context, const RJBindData&, GDSComputeState& computeState, nodeID_t sourceNodeID, RecursiveExtendSharedState* sharedState) override {
        Frontier* frontier = nullptr;
        switch (computeState.frontierPair->getState()) {
        case GDSDensityState::DENSE: {
            frontier = &computeState.frontierPair->ptrCast<SPFrontierPair>()->getDenseFrontier();
        } break;
        case GDSDensityState::SPARSE: {
            frontier = &computeState.visitedSparseFrontier;
        } break;
        default:
            KU_UNREACHABLE;
        }
        return std::make_unique<SSPDestinationsOutputWriter>(context->clientContext, sharedState->getOutputNodeMaskMap(), sourceNodeID, frontier);
    }
};

std::unique_ptr<RJAlgorithm> SingleSPDestinationsFunction::getAlgorithm() {
    return std::make_unique<SingleSPDestinationsAlgorithm>();
}

} // namespace function
} // namespace kuzu
