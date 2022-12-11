#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalSetNodeProperty : public LogicalOperator {
public:
    LogicalSetNodeProperty(vector<expression_pair> setItems, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SET_NODE_PROPERTY, std::move(child)},
          setItems{std::move(setItems)} {}

    inline string getExpressionsForPrinting() const override {
        string result;
        for (auto& [propertyExpression, expression] : setItems) {
            result += propertyExpression->getRawName() + " = " + expression->getRawName() + ",";
        }
        return result;
    }

    inline vector<expression_pair> getSetItems() const { return setItems; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSetNodeProperty>(setItems, children[0]->copy());
    }

private:
    // Property expression = target expression pair.
    vector<expression_pair> setItems;
};

} // namespace planner
} // namespace kuzu
