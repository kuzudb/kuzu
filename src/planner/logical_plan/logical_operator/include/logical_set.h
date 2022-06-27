#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalSet : public LogicalOperator {
public:
    LogicalSet(vector<expression_pair> setItems, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, setItems{move(setItems)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SET;
    }

    inline string getExpressionsForPrinting() const override {
        string result;
        for (auto& [propertyExpression, expression] : setItems) {
            result += propertyExpression->getRawName() + " = " + expression->getRawName() + ",";
        }
        return result;
    }

    inline vector<expression_pair> getSetItems() const { return setItems; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSet>(setItems, children[0]->copy());
    }

private:
    vector<expression_pair> setItems;
};

} // namespace planner
} // namespace graphflow
