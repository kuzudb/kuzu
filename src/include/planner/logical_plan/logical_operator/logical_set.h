#pragma once

#include "logical_update.h"
#include "planner/logical_plan/logical_operator/flatten_resolver.h"

namespace kuzu {
namespace planner {

class LogicalSetNodeProperty : public LogicalUpdateNode {
public:
    LogicalSetNodeProperty(std::vector<std::shared_ptr<binder::NodeExpression>> nodes,
        std::vector<binder::expression_pair> setItems, std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::SET_NODE_PROPERTY, std::move(nodes),
              std::move(child)},
          setItems{std::move(setItems)} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override {
        std::string result;
        for (auto& [lhs, rhs] : setItems) {
            result += lhs->toString() + " = " + rhs->toString() + ",";
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

class LogicalSetRelProperty : public LogicalUpdateRel {
public:
    LogicalSetRelProperty(std::vector<std::shared_ptr<binder::RelExpression>> rels,
        std::vector<binder::expression_pair> setItems, std::shared_ptr<LogicalOperator> child)
        : LogicalUpdateRel{LogicalOperatorType::SET_REL_PROPERTY, std::move(rels),
              std::move(child)},
          setItems{std::move(setItems)} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    f_group_pos_set getGroupsPosToFlatten(uint32_t setItemIdx);

    inline std::string getExpressionsForPrinting() const override {
        std::string result;
        for (auto& [lhs, rhs] : setItems) {
            result += lhs->toString() + " = " + rhs->toString() + ",";
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

} // namespace planner
} // namespace kuzu
