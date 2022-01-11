#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalOrderBy : public LogicalOperator {

public:
    LogicalOrderBy(vector<string> orderByExpressionNames, vector<bool> sortOrders,
        unique_ptr<Schema> schemaBeforeOrderBy, unordered_set<string> expressionToMaterializeNames,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, orderByExpressionNames{move(orderByExpressionNames)},
          isAscOrders{move(sortOrders)}, schemaBeforeOrderBy{move(schemaBeforeOrderBy)},
          expressionToMaterializeNames{move(expressionToMaterializeNames)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_ORDER_BY;
    }

    string getExpressionsForPrinting() const override {
        assert(!orderByExpressionNames.empty());
        auto result = orderByExpressionNames[0];
        for (auto i = 1u; i < orderByExpressionNames.size(); ++i) {
            result += ", " + orderByExpressionNames[i];
        }
        return result;
    }

    inline vector<string> getOrderByExpressionNames() const { return orderByExpressionNames; }

    inline vector<bool> getIsAscOrders() const { return isAscOrders; }

    inline Schema* getSchemaBeforeOrderBy() const { return schemaBeforeOrderBy.get(); }

    inline unordered_set<string> getExpressionToMaterializeNames() const {
        return expressionToMaterializeNames;
    }
    inline void addExpressionToMaterialize(const string& name) {
        expressionToMaterializeNames.insert(name);
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(orderByExpressionNames, isAscOrders,
            schemaBeforeOrderBy->copy(), expressionToMaterializeNames, children[0]->copy());
    }

private:
    vector<string> orderByExpressionNames;
    vector<bool> isAscOrders;
    unique_ptr<Schema> schemaBeforeOrderBy;

    unordered_set<string> expressionToMaterializeNames;
};

} // namespace planner
} // namespace graphflow
