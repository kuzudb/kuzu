#pragma once

#include "function/table/bind_data.h"
#include "function/table_functions.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalTableFunctionCall : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::TABLE_FUNCTION_CALL;

public:
    LogicalTableFunctionCall(function::TableFunction tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, binder::expression_vector columns,
        std::shared_ptr<binder::Expression> offset)
        : LogicalOperator{operatorType_}, tableFunc{tableFunc}, bindData{std::move(bindData)},
          columns{std::move(columns)}, offset{std::move(offset)} {}

    const function::TableFunction& getTableFunc() const { return tableFunc; }
    const function::TableFuncBindData* getBindData() const { return bindData.get(); }

    void setColumnSkips(std::vector<bool> columnSkips) {
        bindData->setColumnSkips(std::move(columnSkips));
    }
    void setColumnPredicates(std::vector<storage::ColumnPredicateSet> predicates) {
        bindData->setColumnPredicates(std::move(predicates));
    }

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    binder::expression_vector getColumns() const { return columns; }

    std::shared_ptr<binder::Expression> getOffset() const { return offset; }

    std::string getExpressionsForPrinting() const override { return tableFunc.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalTableFunctionCall>(tableFunc, bindData->copy(), columns,
            offset);
    }

private:
    function::TableFunction tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    binder::expression_vector columns;
    std::shared_ptr<binder::Expression> offset;
};

} // namespace planner
} // namespace kuzu
