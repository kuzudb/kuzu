#pragma once

#include "function/table_functions.h"
#include "gds_call.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

class GDSCallWorker : public Sink {
    static constexpr PhysicalOperatorType operatorType_ = PhysicalOperatorType::GDS_CALL;

public:
    GDSCallWorker(GDSCallInfo info, std::shared_ptr<GDSCallSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, uint32_t id,
        const std::string& paramString)
        : Sink{std::move(resultSetDescriptor), operatorType_, id, paramString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    // used when cloning the operator
    GDSCallWorker(GDSCallInfo info, function::table_func_t tableFunc,
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

    void setFuncToExecute(function::table_func_t tableFunc) { this->funcToExecute = tableFunc; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<GDSCallWorker>(info.copy(), funcToExecute, sharedState,
            resultSetDescriptor->copy(), id, paramsString);
    }

private:
    GDSCallInfo info;
    function::table_func_t funcToExecute;
    std::shared_ptr<GDSCallSharedState> sharedState;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    std::vector<common::ValueVector*> outputVectors;
    std::unique_ptr<FactorizedTable> localFTable;
};

} // namespace processor
} // namespace kuzu
