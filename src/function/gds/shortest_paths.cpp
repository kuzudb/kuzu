#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "function/gds/frontier.h"
#include "function/gds/gds.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;

namespace kuzu {
namespace function {

struct ShortestPathBindData final : public GDSBindData {
    uint8_t upperBound;

    explicit ShortestPathBindData(uint8_t upperBound) : upperBound{upperBound} {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ShortestPathBindData>(upperBound);
    }
};

struct ShortestPathSourceState {
    nodeID_t sourceNodeID;
    Frontier currentFrontier;
    Frontier nextFrontier;
    // Visited state.
    common::offset_t numVisited = 0;
    std::vector<int64_t> visitedArray;

    explicit ShortestPathSourceState(nodeID_t sourceNodeID, common::offset_t numNodes)
        : sourceNodeID{sourceNodeID} {
        visitedArray.resize(numNodes);
        for (auto i = 0u; i < numNodes; ++i) {
            visitedArray[i] = -1;
        }
        currentFrontier = Frontier();
        currentFrontier.addNode(sourceNodeID, 1 /* multiplicity */);
        nextFrontier = Frontier();
    }

    bool allVisited() const { return numVisited == visitedArray.size(); }
    bool visited(offset_t offset) const { return visitedArray[offset] != -1; }
    void markVisited(nodeID_t nodeID, int64_t length) {
        numVisited++;
        visitedArray[nodeID.offset] = length;
    }
    int64_t getLength(offset_t offset) const { return visitedArray[offset]; }

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

    void materialize(graph::Graph* graph, const ShortestPathSourceState& sourceState,
        FactorizedTable& table) const {
        srcNodeIDVector->setValue<nodeID_t>(0, sourceState.sourceNodeID);
        for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
            if (!sourceState.visited(offset)) {
                continue;
            }
            dstNodeIDVector->setValue<nodeID_t>(0, {offset, graph->getNodeTableID()});
            lengthVector->setValue<int64_t>(0, sourceState.getLength(offset));
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

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::INT64};
    }

    std::vector<std::string> getResultColumnNames() const override {
        return {"src", "dst", "length"};
    }

    std::vector<common::LogicalType> getResultColumnTypes() const override {
        return {*LogicalType::INTERNAL_ID(), *LogicalType::INTERNAL_ID(), *LogicalType::INT64()};
    }

    void bind(const binder::expression_vector& params) override {
        ExpressionUtil::validateExpressionType(*params[1], ExpressionType::LITERAL);
        auto upperBound = params[1]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        bindData = std::make_unique<ShortestPathBindData>(upperBound);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ShortestPathLocalState>(context);
    }

    void exec() override {
        auto extraData = bindData->ptrCast<ShortestPathBindData>();
        auto shortestPathLocalState = localState->ptrCast<ShortestPathLocalState>();
        for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
            auto sourceNodeID = nodeID_t{offset, graph->getNodeTableID()};
            auto sourceState = ShortestPathSourceState(sourceNodeID, graph->getNumNodes());
            // Start recursive computation for current source node ID.
            for (auto currentLevel = 0; currentLevel <= extraData->upperBound; ++currentLevel) {
                if (sourceState.allVisited()) {
                    continue;
                }
                // Compute next frontier.
                for (auto currentNodeID : sourceState.currentFrontier.getNodeIDs()) {
                    if (sourceState.visited(currentNodeID.offset)) {
                        continue;
                    }
                    sourceState.markVisited(currentNodeID, currentLevel);
                    auto nbrs = graph->getNbrs(currentNodeID.offset);
                    for (auto nbr : nbrs) {
                        sourceState.nextFrontier.addNode(nbr, 1);
                    }
                }
                sourceState.initNextFrontier();
            };
            shortestPathLocalState->materialize(graph, sourceState, *table);
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
