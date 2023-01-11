#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDDL : public LogicalOperator {
public:
    LogicalDDL(
        LogicalOperatorType operatorType, string tableName, shared_ptr<Expression> outputExpression)
        : LogicalOperator{operatorType}, tableName{std::move(tableName)},
          outputExpression{std::move(outputExpression)} {}

    inline string getTableName() const { return tableName; }
    inline shared_ptr<Expression> getOutputExpression() const { return outputExpression; }

    inline string getExpressionsForPrinting() const override { return tableName; }

    inline void computeSchema() override {
        schema = make_unique<Schema>();
        auto groupPos = schema->createGroup();
        schema->insertToGroupAndScope(outputExpression, groupPos);
        schema->setGroupAsSingleState(groupPos);
    }

protected:
    string tableName;
    shared_ptr<Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
