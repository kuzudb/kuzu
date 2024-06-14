#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "function/gds/frontier.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;

namespace kuzu {
namespace function {

struct ShortestPathBindData final : public GDSBindData {
    std::shared_ptr<Expression> nodeInput;
    uint8_t upperBound;

    ShortestPathBindData(std::shared_ptr<Expression> nodeInput, uint8_t upperBound)
        : nodeInput{std::move(nodeInput)}, upperBound{upperBound} {}
    ShortestPathBindData(const ShortestPathBindData& other)
        : nodeInput{other.nodeInput}, upperBound{other.upperBound} {}

    bool hasNodeInput() const override { return true; }
    std::shared_ptr<binder::Expression> getNodeInput() const override { return nodeInput; }

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ShortestPathBindData>(*this);
    }
};

struct ShortestPathSourceState {
    nodeID_t sourceNodeID;
    Frontier currentFrontier;
    Frontier nextFrontier;
    // Visited state.
    common::offset_t totalNumNodes = 0;
    common::offset_t numVisited = 0;
    common::node_id_map_t<int64_t> visitedMap;

    explicit ShortestPathSourceState(nodeID_t sourceNodeID, common::offset_t totalNumNodes)
        : sourceNodeID{sourceNodeID}, totalNumNodes{totalNumNodes} {
        currentFrontier = Frontier();
        currentFrontier.addNode(sourceNodeID, 1 /* multiplicity */);
        nextFrontier = Frontier();
    }

    bool allVisited() const { return numVisited == totalNumNodes; }
    bool visited(nodeID_t nodeID) const { return visitedMap.contains(nodeID); }
    void markVisited(nodeID_t nodeID, int64_t length) {
        numVisited++;
        visitedMap.insert({nodeID, length});
    }

    void initNextFrontier() {
        currentFrontier = std::move(nextFrontier);
        nextFrontier = Frontier();
    }
};

class ShortestPathLocalState : public GDSLocalState {
public:
    explicit ShortestPathLocalState(main::ClientContext* context) {
        auto mm = context->getMemoryManager();
        srcNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        dstNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        lengthVector = std::make_unique<ValueVector>(*LogicalType::INT64(), mm);
        srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        dstNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        lengthVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(srcNodeIDVector.get());
        vectors.push_back(dstNodeIDVector.get());
        vectors.push_back(lengthVector.get());
    }

    void materialize(const ShortestPathSourceState& sourceState,
        processor::FactorizedTable& table) const {
        srcNodeIDVector->setValue<nodeID_t>(0, sourceState.sourceNodeID);
        for (auto [dstNodeID, length] : sourceState.visitedMap) {
            dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
            lengthVector->setValue<int64_t>(0, length);
            table.append(vectors);
        }
    }

private:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
    std::vector<ValueVector*> vectors;
};

class ShortestPath final : public GDSAlgorithm {
public:
    ShortestPath() = default;
    ShortestPath(const ShortestPath& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64};
    }

    /*
     * Outputs are
     *
     * srcNode._id::INTERNAL_ID
     * dst::INTERNAL_ID
     * length::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        columns.push_back(bindData->getNodeInput()->constCast<NodeExpression>().getInternalID());
        columns.push_back(binder->createVariable("dst", *LogicalType::INTERNAL_ID()));
        columns.push_back(binder->createVariable("length", *LogicalType::INT64()));
        return columns;
    }

    void bind(const binder::expression_vector& params) override {
        KU_ASSERT(params.size() == 3);
        auto inputNode = params[1];
        ExpressionUtil::validateExpressionType(*params[2], ExpressionType::LITERAL);
        ExpressionUtil::validateDataType(*params[2], *LogicalType::INT64());
        auto upperBound = params[2]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        bindData = std::make_unique<ShortestPathBindData>(inputNode, upperBound);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ShortestPathLocalState>(context);
    }

    void exec() override {
        auto extraData = bindData->ptrCast<ShortestPathBindData>();
        auto shortestPathLocalState = localState->ptrCast<ShortestPathLocalState>();
        auto graph = sharedState->graph.get();
        for (auto& tableID : graph->getNodeTableIDs()) {
            if (!sharedState->inputNodeOffsetMasks.contains(tableID)) {
                continue;
            }
            auto mask = sharedState->inputNodeOffsetMasks.at(tableID).get();
            for (auto offset = 0u; offset < graph->getNumNodes(tableID); ++offset) {
                if (!mask->isNodeMasked(offset)) {
                    continue;
                }
                auto sourceNodeID = nodeID_t{offset, tableID};
                auto sourceState = ShortestPathSourceState(sourceNodeID, graph->getNumNodes());
                // Start recursive computation for current source node ID.
                for (auto currentLevel = 0; currentLevel <= extraData->upperBound; ++currentLevel) {
                    if (sourceState.allVisited()) {
                        continue;
                    }
                    // Compute next frontier.
                    for (auto currentNodeID : sourceState.currentFrontier.getNodeIDs()) {
                        if (sourceState.visited(currentNodeID)) {
                            continue;
                        }
                        sourceState.markVisited(currentNodeID, currentLevel);
                        auto nbrs = graph->scanFwd(currentNodeID);
                        for (auto nbr : nbrs) {
                            sourceState.nextFrontier.addNode(nbr, 1);
                        }
                    }
                    sourceState.initNextFrontier();
                };
                shortestPathLocalState->materialize(sourceState, *sharedState->fTable);
            }
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<ShortestPath>(*this);
    }
};

function_set ShortestPathsFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<ShortestPath>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
