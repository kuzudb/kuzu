#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundStatementResult {
public:
    BoundStatementResult() = default;
    BoundStatementResult(
        expression_vector columns, std::vector<expression_vector> expressionsToCollectPerColumn)
        : columns{std::move(columns)}, expressionsToCollectPerColumn{
                                           std::move(expressionsToCollectPerColumn)} {}

    static std::unique_ptr<BoundStatementResult> createEmptyResult() {
        return std::make_unique<BoundStatementResult>();
    }

    static std::unique_ptr<BoundStatementResult> createSingleStringColumnResult();

    inline void addColumn(
        std::shared_ptr<Expression> column, expression_vector expressionToCollect) {
        columns.push_back(std::move(column));
        expressionsToCollectPerColumn.push_back(std::move(expressionToCollect));
    }
    inline expression_vector getColumns() const { return columns; }
    inline std::vector<expression_vector> getExpressionsToCollectPerColumn() const {
        return expressionsToCollectPerColumn;
    }

    expression_vector getExpressionsToCollect();

    inline std::shared_ptr<Expression> getSingleExpressionToCollect() {
        auto expressionsToCollect = getExpressionsToCollect();
        assert(expressionsToCollect.size() == 1);
        return expressionsToCollect[0];
    }

    inline std::unique_ptr<BoundStatementResult> copy() {
        return std::make_unique<BoundStatementResult>(columns, expressionsToCollectPerColumn);
    }

private:
    expression_vector columns;
    // for node and rel column, we need collect multiple properties and aggregate in json format.
    std::vector<expression_vector> expressionsToCollectPerColumn;
};

} // namespace binder
} // namespace kuzu
