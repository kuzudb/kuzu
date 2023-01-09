#pragma once

#include "binder/expression/expression.h"

namespace kuzu {
namespace binder {

class BoundStatementResult {
public:
    BoundStatementResult() = default;
    BoundStatementResult(
        expression_vector columns, vector<expression_vector> expressionsToCollectPerColumn)
        : columns{std::move(columns)}, expressionsToCollectPerColumn{
                                           std::move(expressionsToCollectPerColumn)} {}

    static unique_ptr<BoundStatementResult> createEmptyResult() {
        return make_unique<BoundStatementResult>();
    }

    static unique_ptr<BoundStatementResult> createSingleStringColumnResult() {
        auto result = make_unique<BoundStatementResult>();
        auto stringColumn = make_shared<Expression>(LITERAL, DataType{STRING}, "outputMsg");
        result->addColumn(stringColumn, expression_vector{stringColumn});
        return result;
    }

    inline void addColumn(shared_ptr<Expression> column, expression_vector expressionToCollect) {
        columns.push_back(std::move(column));
        expressionsToCollectPerColumn.push_back(std::move(expressionToCollect));
    }
    inline expression_vector getColumns() const { return columns; }
    inline vector<expression_vector> getExpressionsToCollectPerColumn() const {
        return expressionsToCollectPerColumn;
    }

    inline expression_vector getExpressionsToCollect() {
        expression_vector result;
        for (auto& expressionsToCollect : expressionsToCollectPerColumn) {
            for (auto& expression : expressionsToCollect) {
                result.push_back(expression);
            }
        }
        return result;
    }

    inline unique_ptr<BoundStatementResult> copy() {
        return make_unique<BoundStatementResult>(columns, expressionsToCollectPerColumn);
    }

private:
    expression_vector columns;
    // for node and rel column, we need collect multiple properties and aggregate in json format.
    vector<expression_vector> expressionsToCollectPerColumn;
};

} // namespace binder
} // namespace kuzu
