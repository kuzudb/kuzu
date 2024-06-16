#pragma once

#include "function/table_functions.h"
#include "gds_call.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "function/gds/ife_morsel.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

using gds_algofunc_t = std::function<uint64_t (std::shared_ptr<GDSCallSharedState>&, GDSLocalState*)>;

class GDSCallWorker : public Sink {
    static constexpr PhysicalOperatorType operatorType_ = PhysicalOperatorType::GDS_CALL;

public:
    GDSCallWorker(GDSCallInfo info, std::shared_ptr<GDSCallSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        const std::string& paramString)
        : Sink{std::move(resultSetDescriptor), operatorType_, id, paramString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    // used when cloning the operator
    GDSCallWorker(GDSCallInfo info, gds_algofunc_t tableFunc,
        std::shared_ptr<GDSCallSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        const std::string& paramString)
        : Sink{std::move(resultSetDescriptor), operatorType_, id, paramString},
          info{std::move(info)}, funcToExecute{tableFunc}, sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    bool isParallel() const override {
        return true;
    }

    void initLocalStateInternal(ResultSet*, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    std::shared_ptr<GDSCallSharedState>& getGDSCallSharedState() {
        return sharedState;
    }

    void setFuncToExecute(gds_algofunc_t algofunc) { this->funcToExecute = algofunc; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<GDSCallWorker>(info.copy(), funcToExecute, sharedState,
            resultSetDescriptor->copy(), id, paramsString);
    }

private:
    GDSCallInfo info;
    gds_algofunc_t funcToExecute;
    std::shared_ptr<GDSCallSharedState> sharedState;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    std::unique_ptr<FactorizedTable> localFTable;
};

} // namespace processor
} // namespace kuzu
