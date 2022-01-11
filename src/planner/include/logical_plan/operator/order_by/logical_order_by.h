#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalOrderBy : public LogicalOperator {

public:
    LogicalOrderBy(vector<shared_ptr<Expression>> expressions, vector<bool> sortOrders,
        unique_ptr<Schema> schemaBeforeOrderBy, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, orderByExpressions{move(expressions)},
          isAscOrders{move(sortOrders)}, schemaBeforeOrderBy{move(schemaBeforeOrderBy)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_ORDER_BY;
    }

    string getExpressionsForPrinting() const override {
        auto result = orderByExpressions[0]->getUniqueName();
        for (auto i = 1u; i < orderByExpressions.size(); ++i) {
            result += ", " + orderByExpressions[i]->getUniqueName();
        }
        return result;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(
            orderByExpressions, isAscOrders, schemaBeforeOrderBy->copy(), children[0]->copy());
    }

    inline vector<shared_ptr<Expression>> getOrderByExpressions() const {
        return orderByExpressions;
    }

    inline vector<bool> getIsAscOrders() const { return isAscOrders; }

    inline unique_ptr<Schema>& getSchemaBeforeOrderBy() { return schemaBeforeOrderBy; }

private:
    vector<shared_ptr<Expression>> orderByExpressions;
    vector<bool> isAscOrders;

public:
    unique_ptr<Schema> schemaBeforeOrderBy;
};

} // namespace planner
} // namespace graphflow
