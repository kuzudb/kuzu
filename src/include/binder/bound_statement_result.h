#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundStatementResult {
public:
    BoundStatementResult() = default;
    explicit BoundStatementResult(expression_vector columns) : columns{std::move(columns)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundStatementResult);

    static BoundStatementResult createEmptyResult() { return BoundStatementResult(); }

    static BoundStatementResult createSingleStringColumnResult(
        const std::string& columnName = "result");

    void addColumn(std::shared_ptr<Expression> column) { columns.push_back(std::move(column)); }
    expression_vector getColumns() const { return columns; }

    std::shared_ptr<Expression> getSingleColumnExpr() const {
        KU_ASSERT(columns.size() == 1);
        return columns[0];
    }

private:
    BoundStatementResult(const BoundStatementResult& other) : columns{other.columns} {}

private:
    expression_vector columns;
};

} // namespace binder
} // namespace kuzu
