#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

using create_item = pair<shared_ptr<Expression> /* node */, vector<expression_pair> /* setItems */>;

class LogicalCreate : public LogicalOperator {
public:
    LogicalCreate(vector<create_item> createItems, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, createItems{move(createItems)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_CREATE;
    }

    inline string getExpressionsForPrinting() const override {
        string result;
        for (auto& createItem : createItems) {
            result += createItem.first->getRawName() + ",";
        }
        return result;
    }

    inline uint32_t getNumCreateItems() const { return createItems.size(); }
    inline create_item getCreateItem(uint32_t idx) const { return createItems[idx]; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCreate>(createItems, children[0]->copy());
    }

private:
    vector<create_item> createItems;
};

} // namespace planner
} // namespace graphflow
