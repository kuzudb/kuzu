#pragma once

#include "planner/operator/logical_operator.h"
#include "planner/operator/schema.h"

namespace kuzu {
namespace planner {

class LogicalDistinct : public LogicalOperator {
public:
    LogicalDistinct(
        binder::expression_vector keyExpressions, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DISTINCT, std::move(child)},
          keyExpressions{std::move(keyExpressions)} {}
    LogicalDistinct(binder::expression_vector keyExpressions,
        binder::expression_vector dependentKeyExpressions, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DISTINCT, std::move(child)},
          keyExpressions{std::move(keyExpressions)}, dependentKeyExpressions{
                                                         std::move(dependentKeyExpressions)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupsPosToFlatten();

    std::string getExpressionsForPrinting() const override;

    inline binder::expression_vector getKeyExpressions() const { return keyExpressions; }
    inline void setKeyExpressions(binder::expression_vector expressions) {
        keyExpressions = std::move(expressions);
    }
    inline binder::expression_vector getDependentKeyExpressions() const {
        return dependentKeyExpressions;
    }
    inline void setDependentKeyExpressions(binder::expression_vector expressions) {
        dependentKeyExpressions = std::move(expressions);
    }
    inline binder::expression_vector getAllDistinctExpressions() const {
        binder::expression_vector result;
        result.insert(result.end(), keyExpressions.begin(), keyExpressions.end());
        result.insert(result.end(), dependentKeyExpressions.begin(), dependentKeyExpressions.end());
        return result;
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDistinct>(
            keyExpressions, dependentKeyExpressions, children[0]->copy());
    }

private:
    binder::expression_vector keyExpressions;
    // See logical_aggregate.h for details.
    binder::expression_vector dependentKeyExpressions;
};

} // namespace planner
} // namespace kuzu
