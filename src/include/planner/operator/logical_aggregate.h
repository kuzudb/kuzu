#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAggregate : public LogicalOperator {
public:
    LogicalAggregate(binder::expression_vector keys, binder::expression_vector aggregates,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::AGGREGATE, std::move(child)}, keys{std::move(keys)},
          aggregates{std::move(aggregates)} {}
    LogicalAggregate(binder::expression_vector keys, binder::expression_vector dependentKeys,
        binder::expression_vector aggregates, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::AGGREGATE, std::move(child)}, keys{std::move(keys)},
          dependentKeys{std::move(dependentKeys)}, aggregates{std::move(aggregates)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupsPosToFlattenForGroupBy();
    f_group_pos_set getGroupsPosToFlattenForAggregate();

    std::string getExpressionsForPrinting() const override;

    bool hasKeys() const { return !keys.empty(); }
    binder::expression_vector getKeys() const { return keys; }
    void setKeys(binder::expression_vector expressions) { keys = std::move(expressions); }
    binder::expression_vector getDependentKeys() const { return dependentKeys; }
    void setDependentKeys(binder::expression_vector expressions) {
        dependentKeys = std::move(expressions);
    }
    binder::expression_vector getAllKeys() const {
        binder::expression_vector result;
        result.insert(result.end(), keys.begin(), keys.end());
        result.insert(result.end(), dependentKeys.begin(), dependentKeys.end());
        return result;
    }
    binder::expression_vector getAggregates() const { return aggregates; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(keys, dependentKeys, aggregates, children[0]->copy());
    }

private:
    bool hasDistinctAggregate();
    void insertAllExpressionsToGroupAndScope(f_group_pos groupPos);

private:
    binder::expression_vector keys;
    // A dependentKeyExpression depend on a keyExpression (e.g. a.age depends on a.ID) and will not
    // be treated as a hash key during hash aggregation.
    binder::expression_vector dependentKeys;
    binder::expression_vector aggregates;
};

} // namespace planner
} // namespace kuzu
