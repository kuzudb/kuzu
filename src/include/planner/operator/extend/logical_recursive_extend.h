#pragma once

#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalRecursiveExtend : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::RECURSIVE_EXTEND;

public:
    LogicalRecursiveExtend(binder::BoundGDSCallInfo info, common::table_id_set_t nbrTableIDSet,
        std::shared_ptr<LogicalOperator> nodeMaskRoot)
        : LogicalOperator{operatorType_}, info{std::move(info)},
          nbrTableIDSet{std::move(nbrTableIDSet)}, limitNum{common::INVALID_LIMIT},
          nodeMaskRoot{std::move(nodeMaskRoot)} {}

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    const binder::BoundGDSCallInfo& getInfo() const { return info; }
    binder::BoundGDSCallInfo& getInfoUnsafe() { return info; }

    void setNbrTableIDSet(common::table_id_set_t set) { nbrTableIDSet = std::move(set); }
    bool hasNbrTableIDSet() const { return !nbrTableIDSet.empty(); }
    common::table_id_set_t getNbrTableIDSet() const { return nbrTableIDSet; }

    void setLimitNum(common::offset_t num) { limitNum = num; }
    common::offset_t getLimitNum() const { return limitNum; }

    bool hasNodePredicate() const { return nodeMaskRoot != nullptr; }
    std::shared_ptr<LogicalOperator> getNodeMaskRoot() const { return nodeMaskRoot; }

    std::string getExpressionsForPrinting() const override { return info.func.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalRecursiveExtend>(info.copy(), nbrTableIDSet, nodeMaskRoot);
    }

private:
    binder::BoundGDSCallInfo info;
    common::table_id_set_t nbrTableIDSet;
    common::offset_t limitNum; // TODO: remove this once recursive extend is pipelined.
    std::shared_ptr<LogicalOperator> nodeMaskRoot;
};

} // namespace planner
} // namespace kuzu
