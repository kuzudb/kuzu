#pragma once

#include "base_logical_operator.h"
#include "schema.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalOrderBy : public LogicalOperator {
public:
    LogicalOrderBy(expression_vector expressionsToOrderBy, vector<bool> sortOrders,
        expression_vector expressionsToMaterialize, unique_ptr<Schema> schemaBeforeOrderBy,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expressionsToOrderBy{move(expressionsToOrderBy)},
          isAscOrders{move(sortOrders)}, expressionsToMaterialize{move(expressionsToMaterialize)},
          schemaBeforeOrderBy{move(schemaBeforeOrderBy)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_ORDER_BY;
    }

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToOrderBy() const { return expressionsToOrderBy; }
    inline vector<bool> getIsAscOrders() const { return isAscOrders; }
    inline Schema* getSchemaBeforeOrderBy() const { return schemaBeforeOrderBy.get(); }
    inline expression_vector getExpressionsToMaterialize() const {
        return expressionsToMaterialize;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(expressionsToOrderBy, isAscOrders,
            expressionsToMaterialize, schemaBeforeOrderBy->copy(), children[0]->copy());
    }

private:
    expression_vector expressionsToOrderBy;
    vector<bool> isAscOrders;
    expression_vector expressionsToMaterialize;
    unique_ptr<Schema> schemaBeforeOrderBy;
};

} // namespace planner
} // namespace kuzu
