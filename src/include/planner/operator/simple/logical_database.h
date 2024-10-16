#pragma once

#include "logical_simple.h"

namespace kuzu {
namespace planner {

class LogicalDatabase : public LogicalSimple {
public:
    explicit LogicalDatabase(LogicalOperatorType operatorType,
        std::shared_ptr<binder::Expression> outputExpression, std::string dbName)
        : LogicalSimple{operatorType, outputExpression}, dbName{std::move(dbName)} {}

    std::string getDBName() const { return dbName; }

    std::string getExpressionsForPrinting() const override { return dbName; }

protected:
    std::string dbName;
};

} // namespace planner
} // namespace kuzu
