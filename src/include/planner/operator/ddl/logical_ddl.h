#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

// TODO: Now that DDL includes sequence, should rename to not be specified by table
class LogicalDDL : public LogicalOperator {
public:
    LogicalDDL(LogicalOperatorType operatorType, std::string tableName,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{operatorType}, tableName{std::move(tableName)},
          outputExpression{std::move(outputExpression)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getTableName() const { return tableName; }
    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline std::string getExpressionsForPrinting() const override { return tableName; }

protected:
    std::string tableName;
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
