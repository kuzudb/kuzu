#pragma once

#include "common/exception/runtime.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalPropertyCollector : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::PROPERTY_COLLECTOR;

public:
    LogicalPropertyCollector(std::shared_ptr<binder::Expression> nodeID,
        std::shared_ptr<binder::Expression> property, LogicalOperator* op,
        std::shared_ptr<LogicalOperator> child, std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{type_, std::move(child), std::move(printInfo)}, nodeID{std::move(nodeID)},
          property{std::move(property)}, op{op} {}

    void computeFactorizedSchema() override { copyChildSchema(0); }
    void computeFlatSchema() override { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override { return property->toString(); }
    LogicalOperator* getOperator() const { return op; }
    std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    std::shared_ptr<binder::Expression> getProperty() const { return property; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalPropertyCollector>(nodeID, property, op, children[0]->copy(),
            printInfo->copy());
    }

private:
    std::shared_ptr<binder::Expression> nodeID;
    std::shared_ptr<binder::Expression> property;
    LogicalOperator* op;
};

} // namespace planner
} // namespace kuzu
