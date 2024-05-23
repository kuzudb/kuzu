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

struct PageRankBindData final : public GDSBindData {
    double dampingFactor = 0.85;
    int64_t maxIteration = 10;
    double delta = 0.0001; // detect convergence

    PageRankBindData() = default;

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<PageRankBindData>();
    }
};

class PageRankLocalState : public GDSLocalState {
public:
    explicit PageRankLocalState(main::ClientContext* context) {
        auto mm = context->getMemoryManager();
        nodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        rankVector = std::make_unique<ValueVector>(*LogicalType::DOUBLE(), mm);
        nodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        rankVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(nodeIDVector.get());
        vectors.push_back(rankVector.get());
    }

    void materialize(graph::Graph* graph, const std::vector<double>& ranks,
        FactorizedTable& table) const {
        for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
            nodeIDVector->setValue<nodeID_t>(0, {offset, graph->getNodeTableID()});
            rankVector->setValue<double>(0, ranks[offset]);
            table.append(vectors);
        }
    }

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> rankVector;
    std::vector<ValueVector*> vectors;
};

class PageRank final : public GDSAlgorithm {
public:
    PageRank() = default;
    PageRank(const PageRank& other) : GDSAlgorithm{other} {}

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY};
    }

    std::vector<std::string> getResultColumnNames() const override { return {"node_id", "rank"}; }

    std::vector<common::LogicalType> getResultColumnTypes() const override {
        return {*LogicalType::INTERNAL_ID(), *LogicalType::DOUBLE()};
    }

    void bind(const binder::expression_vector&) override {
        bindData = std::make_unique<PageRankBindData>();
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<PageRankLocalState>(context);
    }

    void exec() override {
        auto extraData = bindData->ptrCast<PageRankBindData>();
        auto pageRankLocalState = localState->ptrCast<PageRankLocalState>();
        // Initialize state.
        std::vector<double> ranks;
        ranks.resize(graph->getNumNodes());
        for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
            ranks[offset] = (double)1 / graph->getNumNodes();
        }
        auto dampingValue = (1 - extraData->dampingFactor) / graph->getNumNodes();
        // Compute page rank.
        for (auto i = 0u; i < extraData->maxIteration; ++i) {
            auto change = 0.0;
            for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
                auto rank = 0.0;
                auto nbrs = graph->getNbrs(offset);
                for (auto& nbr : nbrs) {
                    auto numNbrOfNbr = graph->getNbrs(nbr.offset).size();
                    if (numNbrOfNbr == 0) {
                        numNbrOfNbr = graph->getNumNodes();
                    }
                    rank += extraData->dampingFactor * (ranks[nbr.offset] / numNbrOfNbr);
                }
                rank += dampingValue;
                double diff = ranks[offset] - rank;
                change += diff < 0 ? -diff : diff;
                ranks[offset] = rank;
            }
            if (change < extraData->delta) {
                break;
            }
        }
        // Materialize result.
        pageRankLocalState->materialize(graph, ranks, *table);
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
