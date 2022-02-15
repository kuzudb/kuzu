#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalUnion : public LogicalOperator {

public:
    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_UNION_ALL;
    }

    inline string getExpressionsForPrinting() const override {
        auto result = string();
        for (auto& expression : expressionsToUnion) {
            result += ", " + expression->getUniqueName();
        }
        return result;
    }

    inline unique_ptr<LogicalOperator> copy() override { return make_unique<LogicalUnion>(); }

    inline void setExpressionsToUnion(vector<shared_ptr<Expression>> expressionsToUnion) {
        this->expressionsToUnion = move(expressionsToUnion);
    }

    inline vector<shared_ptr<Expression>> getExpressionsToUnion() { return expressionsToUnion; }

private:
    vector<shared_ptr<Expression>> expressionsToUnion;
};

} // namespace planner
} // namespace graphflow
