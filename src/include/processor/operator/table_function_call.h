#pragma once

#include "function/table/bind_data.h"
#include "function/table_functions.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct TableFunctionCallSharedState {
    std::unique_ptr<function::TableFuncSharedState> funcState;
    common::row_idx_t nextRowIdx = 0;
    std::mutex mtx;

    common::row_idx_t getAndIncreaseRowIdx(uint64_t numRows);
};

struct TableFunctionCallLocalState {
    std::unique_ptr<function::TableFuncLocalState> funcState;
    function::TableFuncInput funcInput;
    function::TableFuncOutput funcOutput;
    common::ValueVector* rowOffsetVector;

    TableFunctionCallLocalState() = default;
    DELETE_COPY_DEFAULT_MOVE(TableFunctionCallLocalState);
};

enum class TableScanOutputType : uint8_t {
    EMPTY = 0,
    SINGLE_DATA_CHUNK = 1,
    MULTI_DATA_CHUNK = 2,
};

struct TableFunctionCallInfo {
    function::TableFunction function;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::vector<DataPos> outPosV;
    DataPos rowOffsetPos;
    TableScanOutputType outputType;

    TableFunctionCallInfo() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(TableFunctionCallInfo);

private:
    TableFunctionCallInfo(const TableFunctionCallInfo& other) {
        function = other.function;
        bindData = other.bindData->copy();
        outPosV = other.outPosV;
        rowOffsetPos = other.rowOffsetPos;
        outputType = other.outputType;
    }
};

class TableFunctionCall : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::TABLE_FUNCTION_CALL;

public:
    TableFunctionCall(TableFunctionCallInfo info,
        std::shared_ptr<TableFunctionCallSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, id, std::move(printInfo)}, info{std::move(info)},
          sharedState{std::move(sharedState)} {}
    TableFunctionCall(TableFunctionCallInfo info,
        std::shared_ptr<TableFunctionCallSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    TableFunctionCallSharedState* getSharedState() { return sharedState.get(); }

    bool isSource() const override { return true; }

    bool isParallel() const override { return info.function.canParallelFunc(); }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void initGlobalStateInternal(ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    double getProgress(ExecutionContext* context) const override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<TableFunctionCall>(info.copy(), sharedState, id, printInfo->copy());
    }

private:
    TableFunctionCallInfo info;
    std::shared_ptr<TableFunctionCallSharedState> sharedState;
    TableFunctionCallLocalState localState;
};

} // namespace processor
} // namespace kuzu
