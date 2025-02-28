#pragma once

#include "binder/query/reading_clause/bound_gds_call.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalGDSMaskInfo {
    bool isPathNodeMask = true;
    std::vector<std::shared_ptr<LogicalOperator>> maskRoots;
};

class LogicalGDSCall final : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::GDS_CALL;

public:
    explicit LogicalGDSCall(binder::BoundGDSCallInfo info)
        : LogicalOperator{operatorType_}, info{std::move(info)}, limitNum{common::INVALID_LIMIT} {}

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    const binder::BoundGDSCallInfo& getInfo() const { return info; }
    binder::BoundGDSCallInfo& getInfoUnsafe() { return info; }

    void setNbrTableIDSet(common::table_id_set_t set) { nbrTableIDSet = std::move(set); }
    bool hasNbrTableIDSet() const { return !nbrTableIDSet.empty(); }
    common::table_id_set_t getNbrTableIDSet() const { return nbrTableIDSet; }

    void addPathNodeMask(std::shared_ptr<LogicalOperator> maskRoot) {
        maskInfo.isPathNodeMask = true;
        maskInfo.maskRoots = {std::move(maskRoot)};
    }
    void addGraphNodeMask(std::vector<std::shared_ptr<LogicalOperator>> maskRoots) {
        maskInfo.isPathNodeMask = false;
        maskInfo.maskRoots = std::move(maskRoots);
    }
    bool hasNodePredicate() const { return !maskInfo.maskRoots.empty(); }
    const LogicalGDSMaskInfo& getMaskInfo() const { return maskInfo; }

    void setLimitNum(common::offset_t num) { limitNum = num; }
    common::offset_t getLimitNum() const { return limitNum; }

    std::string getExpressionsForPrinting() const override { return info.func.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        auto result = std::make_unique<LogicalGDSCall>(info.copy());
        result->nbrTableIDSet = nbrTableIDSet;
        result->limitNum = limitNum;
        result->maskInfo = maskInfo;
        return result;
    }

private:
    binder::BoundGDSCallInfo info;
    common::table_id_set_t nbrTableIDSet;
    common::offset_t limitNum;
    LogicalGDSMaskInfo maskInfo;
};

} // namespace planner
} // namespace kuzu
