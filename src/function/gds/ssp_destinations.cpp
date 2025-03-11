#include "binder/expression/node_expression.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/rec_joins.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

class SSPDestinationsOutputWriter : public RJOutputWriter {
public:
    SSPDestinationsOutputWriter(main::ClientContext* context, NodeOffsetMaskMap* outputNodeMask,
        nodeID_t sourceNodeID, std::shared_ptr<PathLengths> pathLengths)
        : RJOutputWriter{context, outputNodeMask, sourceNodeID},
          pathLengths{std::move(pathLengths)} {
        lengthVector = createVector(LogicalType::UINT16());
    }

    void beginWritingOutputsInternal(common::table_id_t tableID) override {
        pathLengths->pinCurFrontierTableID(tableID);
    }

    void write(FactorizedTable& fTable, nodeID_t dstNodeID, LimitCounter* counter) override {
        auto length = pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset);
        dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
        lengthVector->setValue<uint16_t>(0, length);
        fTable.append(vectors);
        if (counter != nullptr) {
            counter->increase(1);
        }
    }

    std::unique_ptr<RJOutputWriter> copy() override {
        return std::make_unique<SSPDestinationsOutputWriter>(context, outputNodeMask, sourceNodeID,
            pathLengths);
    }

private:
    bool skipInternal(nodeID_t dstNodeID) const override {
        return dstNodeID == sourceNodeID ||
               pathLengths->getMaskValueFromCurFrontier(dstNodeID.offset) == PathLengths::UNVISITED;
    }

private:
    std::unique_ptr<ValueVector> lengthVector;
    std::shared_ptr<PathLengths> pathLengths;
};

class SSPDestinationsEdgeCompute : public SPEdgeCompute {
public:
    explicit SSPDestinationsEdgeCompute(SinglePathLengthsFrontierPair* frontierPair)
        : SPEdgeCompute{frontierPair} {};

    std::vector<nodeID_t> edgeCompute(nodeID_t, graph::NbrScanState::Chunk& resultChunk,
        bool) override {
        std::vector<nodeID_t> activeNodes;
        resultChunk.forEach([&](auto nbrNode, auto) {
            if (frontierPair->getPathLengths()->getMaskValueFromNextFrontier(nbrNode.offset) ==
                PathLengths::UNVISITED) {
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
    RJCompState getRJCompState(ExecutionContext* context, nodeID_t sourceNodeID, const RJBindData&,
        processor::RecursiveExtendSharedState* sharedState) override {
        auto clientContext = context->clientContext;
        auto frontier = PathLengths::getUnvisitedFrontier(context, sharedState->graph.get());
        auto outputWriter = std::make_unique<SSPDestinationsOutputWriter>(clientContext,
            sharedState->getOutputNodeMaskMap(), sourceNodeID, frontier);
        auto frontierPair = std::make_unique<SinglePathLengthsFrontierPair>(frontier);
        auto edgeCompute = std::make_unique<SSPDestinationsEdgeCompute>(frontierPair.get());
        auto auxiliaryState = std::make_unique<EmptyGDSAuxiliaryState>();
        auto gdsState = std::make_unique<GDSComputeState>(std::move(frontierPair),
            std::move(edgeCompute), std::move(auxiliaryState), sharedState->getOutputNodeMaskMap());
        return RJCompState(std::move(gdsState), std::move(outputWriter));
    }
};

std::unique_ptr<RJAlgorithm> SingleSPDestinationsFunction::getAlgorithm() {
    return std::make_unique<SingleSPDestinationsAlgorithm>();
}

} // namespace function
} // namespace kuzu
