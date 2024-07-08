#pragma once

#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalGDSCall final : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::GDS_CALL;

public:
    explicit LogicalGDSCall(binder::BoundGDSCallInfo info)
        : LogicalOperator{operatorType_}, info{std::move(info)} {}

    LogicalGDSCall(binder::BoundGDSCallInfo info, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType_, std::move(child)}, info{std::move(info)} {}

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    const binder::BoundGDSCallInfo& getInfo() const { return info; }

    std::string getExpressionsForPrinting() const override { return info.func->name; }

    std::unique_ptr<LogicalOperator> copy() override {
        if (children.empty()) {
            return std::make_unique<LogicalGDSCall>(info.copy());
        } else {
            return std::make_unique<LogicalGDSCall>(info.copy(), children[0]->copy());
        }
    }

private:
    binder::BoundGDSCallInfo info;
};

} // namespace planner
} // namespace kuzu
