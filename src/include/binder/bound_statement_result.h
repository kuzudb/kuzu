#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundStatementResult {
public:
    BoundStatementResult() = default;
    BoundStatementResult(expression_vector columns) : columns{std::move(columns)} {}

    static std::unique_ptr<BoundStatementResult> createEmptyResult() {
        return std::make_unique<BoundStatementResult>();
    }

    static std::unique_ptr<BoundStatementResult> createSingleStringColumnResult(
        std::string columnName = "result");

    inline void addColumn(std::shared_ptr<Expression> column) {
        columns.push_back(std::move(column));
    }
    inline expression_vector getColumns() const { return columns; }

    inline std::shared_ptr<Expression> getSingleExpressionToCollect() {
        KU_ASSERT(columns.size() == 1);
        return columns[0];
    }

    inline std::unique_ptr<BoundStatementResult> copy() {
        return std::make_unique<BoundStatementResult>(columns);
    }

private:
    expression_vector columns;
};

} // namespace binder
} // namespace kuzu
