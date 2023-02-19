#pragma once

#include "logical_update.h"
#include "planner/logical_plan/logical_operator/factorization_resolver.h"

namespace kuzu {
namespace planner {

class LogicalSetNodeProperty : public LogicalUpdateNode {
public:
    LogicalSetNodeProperty(std::vector<std::shared_ptr<binder::NodeExpression>> nodes,
        std::vector<binder::expression_pair> setItems, std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::SET_NODE_PROPERTY, std::move(nodes),
              std::move(child)},
          setItems{std::move(setItems)} {}

    inline std::string getExpressionsForPrinting() const override {
        std::string result;
        for (auto& [lhs, rhs] : setItems) {
            result += lhs->getRawName() + " = " + rhs->getRawName() + ",";
        }
        return result;
    }

    inline binder::expression_pair getSetItem(size_t idx) const { return setItems[idx]; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSetNodeProperty>(nodes, setItems, children[0]->copy());
    }

private:
    std::vector<binder::expression_pair> setItems;
};

class LogicalSetNodePropertyFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlattenForRhs(
        const std::shared_ptr<binder::Expression>& rhs, LogicalOperator* setNodePropertyChild) {
        auto schema = setNodePropertyChild->getSchema();
        auto dependentGroupsPos = schema->getDependentGroupsPos(rhs);
        return FlattenAllButOneFactorizationSolver::getGroupsPosToFlatten(
            dependentGroupsPos, schema);
    }
    static bool requireFlatLhs(const std::shared_ptr<binder::Expression>& lhsNodeID,
        const std::shared_ptr<binder::Expression>& rhs, LogicalOperator* setNodePropertyChild) {
        auto schema = setNodePropertyChild->getSchema();
        auto lhsGroupPos = schema->getGroupPos(*lhsNodeID);
        auto rhsLeadingGroupPos =
            SchemaUtils::getLeadingGroupPos(schema->getDependentGroupsPos(rhs), *schema);
        return lhsGroupPos != rhsLeadingGroupPos;
    }
};

class LogicalSetRelProperty : public LogicalUpdateRel {
public:
    LogicalSetRelProperty(std::vector<std::shared_ptr<binder::RelExpression>> rels,
        std::vector<binder::expression_pair> setItems, std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateRel{LogicalOperatorType::SET_REL_PROPERTY, std::move(rels),
              std::move(child)},
          setItems{std::move(setItems)} {}

    inline std::string getExpressionsForPrinting() const override {
        std::string result;
        for (auto& [lhs, rhs] : setItems) {
            result += lhs->getRawName() + " = " + rhs->getRawName() + ",";
        }
        return result;
    }

    inline binder::expression_pair getSetItem(size_t idx) const { return setItems[idx]; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSetRelProperty>(rels, setItems, children[0]->copy());
    }

private:
    std::vector<binder::expression_pair> setItems;
};

class LogicalSetRelPropertyFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(
        const std::shared_ptr<binder::RelExpression>& rel,
        const std::shared_ptr<binder::Expression>& rhs, LogicalOperator* setRelPropertyChild) {
        std::unordered_set<f_group_pos> result;
        auto schema = setRelPropertyChild->getSchema();
        result.insert(schema->getGroupPos(*rel->getSrcNode()->getInternalIDProperty()));
        result.insert(schema->getGroupPos(*rel->getDstNode()->getInternalIDProperty()));
        for (auto groupPos : schema->getDependentGroupsPos(rhs)) {
            result.insert(groupPos);
        }
        return FlattenAllFactorizationSolver::getGroupsPosToFlatten(result, schema);
    }
};

} // namespace planner
} // namespace kuzu
