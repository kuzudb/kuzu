#pragma once

#include "function/algorithm/graph_algorithms.h"
#include "function/table_functions.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {

namespace graph {
struct GraphAlgorithm;
} // namespace graph

namespace processor {

class AlgorithmRunnerMain : public Sink {
public:
    AlgorithmRunnerMain(InQueryCallInfo info, std::shared_ptr<InQueryCallSharedState> sharedState,
        std::shared_ptr<graph::GraphAlgorithm> graphAlgorithm, uint32_t id,
        const std::string& paramString)
        : Sink{nullptr, PhysicalOperatorType::ALGORITHM_RUNNER, id, paramString}, info{std::move(
                                                                                      info)},
          sharedState{std::move(sharedState)}, graphAlgorithm{std::move(graphAlgorithm)} {}

    bool isSource() const override { return true; }

    inline bool canParallel() const final { return false; }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<AlgorithmRunnerMain>(info.copy(), sharedState, graphAlgorithm, id,
            paramsString);
    }

private:
    InQueryCallInfo info;
    std::shared_ptr<InQueryCallSharedState> sharedState;
    std::shared_ptr<graph::GraphAlgorithm> graphAlgorithm;
};

} // namespace processor
} // namespace kuzu
