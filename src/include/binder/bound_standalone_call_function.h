#pragma once

#include "binder/bound_statement.h"
#include "bound_table_function.h"

namespace kuzu {
namespace binder {

class BoundStandaloneCallFunction : public BoundStatement {
    static constexpr common::StatementType statementType =
        common::StatementType::STANDALONE_CALL_FUNCTION;

public:
    explicit BoundStandaloneCallFunction(BoundTableFunction tableFunc)
        : BoundStatement{statementType, BoundStatementResult::createEmptyResult()},
          tableFunc{std::move(tableFunc)} {}

    const function::TableFunction& getTableFunction() const { return *tableFunc.tableFunction; }

    function::TableFuncBindData* getBindData() const { return tableFunc.bindData.get(); }

private:
    BoundTableFunction tableFunc;
};

} // namespace binder
} // namespace kuzu
