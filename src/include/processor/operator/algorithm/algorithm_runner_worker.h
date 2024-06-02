#include "function/table_functions.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct AlgorithmRunnerSharedState {

    explicit AlgorithmRunnerSharedState(std::shared_ptr<FactorizedTable> resultFTable)
        : resultFTable{std::move(resultFTable)} {}

    void mergeResults(FactorizedTable* localFTable) {
        std::unique_lock lck{mtx};
        resultFTable->merge(*localFTable);
    }

    std::mutex mtx;
    std::shared_ptr<FactorizedTable> resultFTable;
};

class AlgorithmRunnerWorker : public Sink {
public:
    AlgorithmRunnerWorker(InQueryCallInfo info, std::shared_ptr<InQueryCallSharedState> sharedState,
        std::shared_ptr<AlgorithmRunnerSharedState> algorithmRunnerSharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        const std::string& paramString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::ALGORITHM_RUNNER, id,
              paramString},
          info{std::move(info)}, sharedState{std::move(sharedState)},
          algorithmRunnerSharedState{std::move(algorithmRunnerSharedState)} {}

    bool isSource() const override { return true; }

    inline bool canParallel() const final { return false; }

    void initLocalStateInternal(ResultSet*, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    function::TableFuncSharedState* getInQuerySharedState() {
        return sharedState->funcState.get();
    }

    void incrementTableFuncIdx() {
        sharedState->tableFuncIdx++;
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<AlgorithmRunnerWorker>(info.copy(), sharedState,
            algorithmRunnerSharedState, resultSetDescriptor->copy(), id, paramsString);
    }

private:
    InQueryCallInfo info;
    std::shared_ptr<InQueryCallSharedState> sharedState;
    std::shared_ptr<AlgorithmRunnerSharedState> algorithmRunnerSharedState;
    InQueryCallLocalState localState;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    std::vector<common::ValueVector*> outputVectors;
    std::unique_ptr<FactorizedTable> localFTable;
};

} // namespace processor
} // namespace kuzu
