#pragma once

#include "function/table/bind_data.h"
#include "function/table_functions.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct TableFunctionCallSharedState {
    std::unique_ptr<function::TableFuncSharedState> funcState;
    std::mutex mtx;
};

struct TableFunctionCallLocalState {
    std::unique_ptr<function::TableFuncLocalState> funcState = nullptr;
    function::TableFuncInput funcInput{};
    function::TableFuncOutput funcOutput{};

    TableFunctionCallLocalState() = default;
    DELETE_COPY_DEFAULT_MOVE(TableFunctionCallLocalState);
};

enum class TableScanOutputType : uint8_t {
    EMPTY = 0,
    SINGLE_DATA_CHUNK = 1,
    MULTI_DATA_CHUNK = 2,
};

struct TableFunctionCallInfo {
    function::TableFunction function{};
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::vector<DataPos> outPosV;
    DataPos rowOffsetPos;
    TableScanOutputType outputType = TableScanOutputType::EMPTY;

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

struct TableFunctionCallPrintInfo final : OPPrintInfo {
    std::string funcName;

    explicit TableFunctionCallPrintInfo(std::string funcName) : funcName(std::move(funcName)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<TableFunctionCallPrintInfo>(new TableFunctionCallPrintInfo(*this));
    }

private:
    TableFunctionCallPrintInfo(const TableFunctionCallPrintInfo& other)
        : OPPrintInfo(other), funcName(other.funcName) {}
};

struct FTableScanFunctionCallPrintInfo final : OPPrintInfo {
    std::string funcName;
    binder::expression_vector exprs;

    explicit FTableScanFunctionCallPrintInfo(std::string funcName, binder::expression_vector exprs)
        : funcName(std::move(funcName)), exprs(std::move(exprs)) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<FTableScanFunctionCallPrintInfo>(
            new FTableScanFunctionCallPrintInfo(*this));
    }

private:
    FTableScanFunctionCallPrintInfo(const FTableScanFunctionCallPrintInfo& other)
        : OPPrintInfo(other), funcName(other.funcName), exprs(other.exprs) {}
};

class TableFunctionCall : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::TABLE_FUNCTION_CALL;

public:
    TableFunctionCall(TableFunctionCallInfo info,
        std::shared_ptr<TableFunctionCallSharedState> sharedState, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, id, std::move(printInfo)}, info{std::move(info)},
          sharedState{std::move(sharedState)} {}
    // When exporting a kuzu database, we may have multiple copy to statements as children of the
    // FactorizedTableScan function.
    TableFunctionCall(TableFunctionCallInfo info,
        std::shared_ptr<TableFunctionCallSharedState> sharedState, physical_op_vector_t children,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(children), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    TableFunctionCallSharedState* getSharedState() { return sharedState.get(); }

    bool isSource() const override { return true; }

    bool isParallel() const override { return info.function.canParallelFunc(); }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void initGlobalStateInternal(ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    void finalizeInternal(ExecutionContext* context) override;

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
