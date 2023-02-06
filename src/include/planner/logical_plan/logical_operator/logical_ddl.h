#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDDL : public LogicalOperator {
public:
    LogicalDDL(LogicalOperatorType operatorType, std::string tableName,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{operatorType}, tableName{std::move(tableName)},
          outputExpression{std::move(outputExpression)} {}

    inline std::string getTableName() const { return tableName; }
    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline std::string getExpressionsForPrinting() const override { return tableName; }

    inline void computeSchema() override {
        schema = std::make_unique<Schema>();
        auto groupPos = schema->createGroup();
        schema->insertToGroupAndScope(outputExpression, groupPos);
        schema->setGroupAsSingleState(groupPos);
    }

protected:
    std::string tableName;
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
