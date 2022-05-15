#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace graphflow {
namespace planner {

class LogicalSet : public LogicalOperator {

public:
    LogicalSet(vector<pair<shared_ptr<Expression>, shared_ptr<Expression>>> setItems,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, setItems{move(setItems)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SET;
    }

    string getExpressionsForPrinting() const override {
        string result;
        for (auto& [propertyExpression, expression] : setItems) {
            result += propertyExpression->getRawName() + " = " + expression->getRawName() + ",";
        }
        return result;
    }

    inline vector<pair<shared_ptr<Expression>, shared_ptr<Expression>>> getSetItems() const {
        return setItems;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSet>(setItems, children[0]->copy());
    }

private:
    vector<pair<shared_ptr<Expression> /* propertyExpression */,
        shared_ptr<Expression> /* target expression */>>
        setItems;
};

} // namespace planner
} // namespace graphflow
