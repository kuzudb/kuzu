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

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    const binder::BoundGDSCallInfo& getInfo() const { return info; }

    bool hasNodePredicate() const { return getNumChildren() > 0; }
    std::vector<std::shared_ptr<LogicalOperator>> getNodeMaskRoots() const {
        std::vector<std::shared_ptr<LogicalOperator>> nodeMaskRoots;
        for (const auto& child : children) {
            KU_ASSERT(child->getOperatorType() == LogicalOperatorType::DUMMY_SINK);
            nodeMaskRoots.push_back(child);
        }
        return nodeMaskRoots;
    }

    std::string getExpressionsForPrinting() const override { return info.func.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalGDSCall>(info.copy());
    }

private:
    binder::BoundGDSCallInfo info;
};

} // namespace planner
} // namespace kuzu
