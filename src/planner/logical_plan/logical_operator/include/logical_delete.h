#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalDelete : public LogicalOperator {
public:
    LogicalDelete(expression_vector nodeExpressions, expression_vector primaryKeyExpressions,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, nodeExpressions{move(nodeExpressions)},
          primaryKeyExpressions{move(primaryKeyExpressions)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_DELETE;
    }

    string getExpressionsForPrinting() const override {
        string result;
        for (auto& nodeExpression : nodeExpressions) {
            result += nodeExpression->getRawName() + ",";
        }
        return result;
    }

    inline uint32_t getNumExpressions() const { return nodeExpressions.size(); }
    inline shared_ptr<Expression> getNodeExpression(uint32_t idx) const {
        return nodeExpressions[idx];
    }
    inline shared_ptr<Expression> getPrimaryKeyExpression(uint32_t idx) const {
        return primaryKeyExpressions[idx];
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDelete>(
            nodeExpressions, primaryKeyExpressions, children[0]->copy());
    }

private:
    expression_vector nodeExpressions;
    expression_vector primaryKeyExpressions;
};

} // namespace planner
} // namespace kuzu
