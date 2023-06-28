#pragma once

#include "binder/expression/expression.h"
#include "function/table_operations.h"

namespace kuzu {
namespace binder {

class BoundCallTableFunc : public BoundStatement {
public:
    BoundCallTableFunc(function::table_func_t tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData,
        std::unique_ptr<BoundStatementResult> statementResult)
        : BoundStatement{common::StatementType::CALL_TABLE_FUNC, std::move(statementResult)},
          tableFunc{std::move(tableFunc)}, bindData{std::move(bindData)} {}

    inline function::table_func_t getTableFunc() const { return tableFunc; }

    inline function::TableFuncBindData* getBindData() const { return bindData.get(); }

private:
    function::table_func_t tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
};

} // namespace binder
} // namespace kuzu
