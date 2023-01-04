#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(shared_ptr<Expression> nodeID, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)}, nodeID{std::move(
                                                                                   nodeID)} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline string getExpressionsForPrinting() const override { return nodeID->getRawName(); }

    inline shared_ptr<Expression> getNodeID() const { return nodeID; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSemiMasker>(nodeID, children[0]->copy());
    }

private:
    shared_ptr<Expression> nodeID;
};

} // namespace planner
} // namespace kuzu
