#pragma once

#include "logical_update.h"

namespace kuzu {
namespace planner {

class LogicalSetNodeProperty : public LogicalUpdateNode {
public:
    LogicalSetNodeProperty(vector<shared_ptr<NodeExpression>> nodes,
        vector<expression_pair> setItems, shared_ptr<LogicalOperator> child)
        : LogicalUpdateNode{LogicalOperatorType::SET_NODE_PROPERTY, std::move(nodes),
              std::move(child)},
          setItems{std::move(setItems)} {}

    inline string getExpressionsForPrinting() const override {
        string result;
        for (auto& [lhs, rhs] : setItems) {
            result += lhs->getRawName() + " = " + rhs->getRawName() + ",";
        }
        return result;
    }

    inline expression_pair getSetItem(size_t idx) const { return setItems[idx]; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSetNodeProperty>(nodes, setItems, children[0]->copy());
    }

private:
    vector<expression_pair> setItems;
};

class LogicalSetRelProperty : public LogicalUpdateRel {
public:
    LogicalSetRelProperty(vector<shared_ptr<RelExpression>> rels, vector<expression_pair> setItems,
        shared_ptr<LogicalOperator> child)
        : LogicalUpdateRel{LogicalOperatorType::SET_REL_PROPERTY, std::move(rels),
              std::move(child)},
          setItems{std::move(setItems)} {}

    inline string getExpressionsForPrinting() const override {
        string result;
        for (auto& [lhs, rhs] : setItems) {
            result += lhs->getRawName() + " = " + rhs->getRawName() + ",";
        }
        return result;
    }

    inline expression_pair getSetItem(size_t idx) const { return setItems[idx]; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSetRelProperty>(rels, setItems, children[0]->copy());
    }

private:
    vector<expression_pair> setItems;
};

} // namespace planner
} // namespace kuzu
