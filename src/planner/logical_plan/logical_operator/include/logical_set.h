#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace planner {

class LogicalSetNodeProperty : public LogicalOperator {
public:
    LogicalSetNodeProperty(
        vector<expression_pair> setItems, bool isUnstructured, shared_ptr<LogicalOperator> child)
        : LogicalOperator{std::move(child)}, setItems{std::move(setItems)}, isUnstructured{
                                                                                isUnstructured} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SET_NODE_PROPERTY;
    }

    inline string getExpressionsForPrinting() const override {
        string result;
        for (auto& [propertyExpression, expression] : setItems) {
            result += propertyExpression->getRawName() + " = " + expression->getRawName() + ",";
        }
        return result;
    }

    inline vector<expression_pair> getSetItems() const { return setItems; }
    inline bool getIsUnstructured() const { return isUnstructured; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSetNodeProperty>(setItems, isUnstructured, children[0]->copy());
    }

private:
    // Property expression = target expression pair.
    vector<expression_pair> setItems;
    // Whether all properties to set is unstructured or not. LogicalSetNodeProperty will be compiled
    // to either ScanStructuredNodeProperty or ScanUnstructuredNodeProperty.
    bool isUnstructured;
};

} // namespace planner
} // namespace graphflow
