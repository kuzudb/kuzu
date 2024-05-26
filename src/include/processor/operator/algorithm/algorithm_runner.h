#pragma once

#include "function/algorithm/graph_algorithms.h"
#include "function/algorithm/parallel_utils.h"
#include "function/table_functions.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

class AlgorithmRunner : public Sink {
public:
    AlgorithmRunner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, InQueryCallInfo info,
        std::shared_ptr<InQueryCallSharedState> sharedState,
        std::unique_ptr<FactorizedTableSchema> tableSchema,
        std::shared_ptr<graph::GraphAlgorithm> graphAlgorithm,
        std::shared_ptr<graph::ParallelUtils> parallelUtils, uint32_t id,
        const std::string& paramString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::ALGORITHM_RUNNER, id,
              paramString},
          info{std::move(info)}, sharedState{std::move(sharedState)}, tableSchema{std::move(
                                                                          tableSchema)},
          graphAlgorithm{std::move(graphAlgorithm)}, parallelUtils{std::move(parallelUtils)} {}

    bool isSource() const override { return true; }

    inline bool canParallel() const final { return false; }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet*, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    void runWorker();

    inline function::TableFuncSharedState* getFuncSharedState() {
        return sharedState->funcState.get();
    }

    inline void incrementTableFuncIdx() {
        sharedState->tableFuncIdx++;
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<AlgorithmRunner>(resultSetDescriptor->copy(), info.copy(),
            sharedState, tableSchema->copy(), graphAlgorithm, parallelUtils, id, paramsString);
    }

private:
    bool isMaster;
    InQueryCallInfo info;
    std::shared_ptr<InQueryCallSharedState> sharedState;
    InQueryCallLocalState localState;
    std::vector<common::ValueVector*> outputVectors;
    std::unique_ptr<FactorizedTable> localFTable;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    std::shared_ptr<graph::GraphAlgorithm> graphAlgorithm;
    std::shared_ptr<graph::ParallelUtils> parallelUtils;
};

} // namespace processor
} // namespace kuzu
