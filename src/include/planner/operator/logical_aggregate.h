#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAggregate : public LogicalOperator {
public:
    LogicalAggregate(binder::expression_vector keyExpressions,
        binder::expression_vector aggregateExpressions, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::AGGREGATE, std::move(child)},
          keyExpressions{std::move(keyExpressions)}, aggregateExpressions{
                                                         std::move(aggregateExpressions)} {}
    LogicalAggregate(binder::expression_vector keyExpressions,
        binder::expression_vector dependentKeyExpressions,
        binder::expression_vector aggregateExpressions, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::AGGREGATE, std::move(child)},
          keyExpressions{std::move(keyExpressions)}, dependentKeyExpressions{std::move(
                                                         dependentKeyExpressions)},
          aggregateExpressions{std::move(aggregateExpressions)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupsPosToFlattenForGroupBy();
    f_group_pos_set getGroupsPosToFlattenForAggregate();

    std::string getExpressionsForPrinting() const override;

    inline bool hasKeyExpressions() const { return !keyExpressions.empty(); }
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
    inline binder::expression_vector getAllKeyExpressions() const {
        binder::expression_vector result;
        result.insert(result.end(), keyExpressions.begin(), keyExpressions.end());
        result.insert(result.end(), dependentKeyExpressions.begin(), dependentKeyExpressions.end());
        return result;
    }
    inline binder::expression_vector getAggregateExpressions() const {
        return aggregateExpressions;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(
            keyExpressions, dependentKeyExpressions, aggregateExpressions, children[0]->copy());
    }

private:
    bool hasDistinctAggregate();
    void insertAllExpressionsToGroupAndScope(f_group_pos groupPos);

private:
    binder::expression_vector keyExpressions;
    // A dependentKeyExpression depend on a keyExpression (e.g. a.age depends on a.ID) and will not
    // be treated as a hash key during hash aggregation.
    binder::expression_vector dependentKeyExpressions;
    binder::expression_vector aggregateExpressions;
};

} // namespace planner
} // namespace kuzu
