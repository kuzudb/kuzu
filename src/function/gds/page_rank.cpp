#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/types/internal_id_util.h"
#include "function/gds/gds.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct PageRankBindData final : public GDSBindData {
    double dampingFactor = 0.85;
    int64_t maxIteration = 10;
    double delta = 0.0001; // detect convergence

    PageRankBindData(std::shared_ptr<binder::Expression> nodeOutput, bool outputAsNode)
        : GDSBindData{std::move(nodeOutput), outputAsNode} {};
    PageRankBindData(const PageRankBindData& other)
        : GDSBindData{other}, dampingFactor{other.dampingFactor}, maxIteration{other.maxIteration},
          delta{other.delta} {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<PageRankBindData>(*this);
    }
};

class PageRankLocalState : public GDSLocalState {
public:
    explicit PageRankLocalState(main::ClientContext* context) {
        auto mm = context->getMemoryManager();
        nodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        rankVector = std::make_unique<ValueVector>(LogicalType::DOUBLE(), mm);
        nodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        rankVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(nodeIDVector.get());
        vectors.push_back(rankVector.get());
    }

    void materialize(graph::Graph* graph, const common::node_id_map_t<double>& ranks,
        FactorizedTable& table) const {
        for (auto tableID : graph->getNodeTableIDs()) {
            for (auto offset = 0u; offset < graph->getNumNodes(tableID); ++offset) {
                auto nodeID = nodeID_t{offset, tableID};
                nodeIDVector->setValue<nodeID_t>(0, nodeID);
                rankVector->setValue<double>(0, ranks.at(nodeID));
                table.append(vectors);
            }
        }
    }

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> rankVector;
    std::vector<ValueVector*> vectors;
};

class PageRank final : public GDSAlgorithm {
    static constexpr char RANK_COLUMN_NAME[] = "rank";

public:
    PageRank() = default;
    PageRank(const PageRank& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * graph::ANY
     * outputProperty::BOOL
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::BOOL};
    }

    /*
     * Outputs are
     *
     * node_id::INTERNAL_ID
     * rank::DOUBLE
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(binder->createVariable(RANK_COLUMN_NAME, LogicalType::DOUBLE()));
        return columns;
    }

    void bind(const expression_vector& params, Binder* binder, GraphEntry& graphEntry) override {
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[1]);
        bindData = std::make_unique<PageRankBindData>(nodeOutput, outputProperty);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<PageRankLocalState>(context);
    }

    void exec(processor::ExecutionContext*) override {
        auto extraData = bindData->ptrCast<PageRankBindData>();
        auto pageRankLocalState = localState->ptrCast<PageRankLocalState>();
        auto graph = sharedState->graph.get();
        // Initialize state.
        common::node_id_map_t<double> ranks;
        auto numNodes = graph->getNumNodes();
        for (auto tableID : graph->getNodeTableIDs()) {
            for (auto offset = 0u; offset < graph->getNumNodes(tableID); ++offset) {
                auto nodeID = nodeID_t{offset, tableID};
                ranks.insert({nodeID, 1.0 / numNodes});
            }
        }
        auto dampingValue = (1 - extraData->dampingFactor) / numNodes;
        // Compute page rank.
        for (auto i = 0u; i < extraData->maxIteration; ++i) {
            auto change = 0.0;
            for (auto tableID : graph->getNodeTableIDs()) {
                for (auto offset = 0u; offset < graph->getNumNodes(tableID); ++offset) {
                    auto nodeID = nodeID_t{offset, tableID};
                    auto rank = 0.0;
                    auto nbrs = graph->scanFwd(nodeID);
                    for (auto& nbr : nbrs) {
                        auto numNbrOfNbr = graph->scanFwd(nbr).size();
                        if (numNbrOfNbr == 0) {
                            numNbrOfNbr = graph->getNumNodes();
                        }
                        rank += extraData->dampingFactor * (ranks[nbr] / numNbrOfNbr);
                    }
                    rank += dampingValue;
                    double diff = ranks[nodeID] - rank;
                    change += diff < 0 ? -diff : diff;
                    ranks[nodeID] = rank;
                }
            }
            if (change < extraData->delta) {
                break;
            }
        }
        // Materialize result.
        pageRankLocalState->materialize(graph, ranks, *sharedState->fTable);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<PageRank>(*this);
    }
};

function_set PageRankFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<PageRank>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
