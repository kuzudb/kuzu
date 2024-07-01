#pragma once

#include "function/gds/ife_morsel.h"
#include "function/table_functions.h"
#include "gds_call.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

using gds_algofunc_t = std::function<uint64_t(GDSCallSharedState*, GDSLocalState*)>;

class ParallelUtilsOperator : public Sink {
    static constexpr PhysicalOperatorType operatorType_ = PhysicalOperatorType::GDS_CALL;

public:
    ParallelUtilsOperator(std::unique_ptr<GDSLocalState> gdsLocalState, gds_algofunc_t tableFunc,
        GDSCallSharedState* sharedState, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{nullptr /* no result descriptor needed */, operatorType_, id, std::move(printInfo)},
          gdsLocalState{std::move(gdsLocalState)}, funcToExecute{tableFunc},
          sharedState{sharedState} {}

    bool isSource() const override { return true; }

    bool isParallel() const override { return true; }

    void initLocalStateInternal(ResultSet*, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    inline uint64_t getWork() override { return gdsLocalState->getWork(); }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ParallelUtilsOperator>(gdsLocalState->copy(), funcToExecute,
            sharedState, id, printInfo->copy());
    }

private:
    std::unique_ptr<GDSLocalState> gdsLocalState;
    gds_algofunc_t funcToExecute;
    GDSCallSharedState* sharedState;
    std::unique_ptr<FactorizedTable> localFTable;
};

} // namespace processor
} // namespace kuzu
