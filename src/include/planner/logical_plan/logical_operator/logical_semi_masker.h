#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(
        std::shared_ptr<binder::Expression> nodeID, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)}, nodeID{std::move(
                                                                                   nodeID)} {}

    inline void computeSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override { return nodeID->getRawName(); }

    inline std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSemiMasker>(nodeID, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> nodeID;
};

} // namespace planner
} // namespace kuzu
