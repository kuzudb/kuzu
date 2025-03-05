#pragma once

#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalGDSCall final : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::GDS_CALL;

public:
    explicit LogicalGDSCall(binder::BoundGDSCallInfo info,
        std::vector<std::shared_ptr<LogicalOperator>> nodeMaskRoots)
        : LogicalOperator{operatorType_}, info{std::move(info)},
          nodeMaskRoots{std::move(nodeMaskRoots)} {}

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    const binder::BoundGDSCallInfo& getInfo() const { return info; }

    bool hasNodePredicate() const { return !nodeMaskRoots.empty(); }
    std::vector<std::shared_ptr<LogicalOperator>> getNodeMaskRoots() const { return nodeMaskRoots; }

    std::string getExpressionsForPrinting() const override { return info.func.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalGDSCall>(info.copy(), nodeMaskRoots);
    }

private:
    binder::BoundGDSCallInfo info;
    std::vector<std::shared_ptr<LogicalOperator>> nodeMaskRoots;
};

} // namespace planner
} // namespace kuzu
