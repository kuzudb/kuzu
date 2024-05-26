#pragma once

#include "binder/query/updating_clause/bound_set_info.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/persistent/logical_insert.h"

namespace kuzu {
namespace planner {

class LogicalMerge final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::MERGE;

public:
    LogicalMerge(std::shared_ptr<binder::Expression> existenceMark,
        std::shared_ptr<binder::Expression> distinctMark, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{type_, std::move(child)}, existenceMark{std::move(existenceMark)},
          distinctMark{std::move(distinctMark)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final { return {}; }

    f_group_pos_set getGroupsPosToFlatten();

    std::shared_ptr<binder::Expression> getExistenceMark() const { return existenceMark; }
    bool hasDistinctMark() const { return distinctMark != nullptr; }
    std::shared_ptr<binder::Expression> getDistinctMark() const { return distinctMark; }

    void addInsertNodeInfo(LogicalInsertInfo info) { insertNodeInfos.push_back(std::move(info)); }
    const std::vector<LogicalInsertInfo>& getInsertNodeInfos() const { return insertNodeInfos; }

    void addInsertRelInfo(LogicalInsertInfo info) { insertRelInfos.push_back(std::move(info)); }
    const std::vector<LogicalInsertInfo>& getInsertRelInfos() const { return insertRelInfos; }

    void addOnCreateSetNodeInfo(binder::BoundSetPropertyInfo info) {
        onCreateSetNodeInfos.push_back(std::move(info));
    }
    const std::vector<binder::BoundSetPropertyInfo>& getOnCreateSetNodeInfos() const {
        return onCreateSetNodeInfos;
    }

    void addOnCreateSetRelInfo(binder::BoundSetPropertyInfo info) {
        onCreateSetRelInfos.push_back(std::move(info));
    }
    const std::vector<binder::BoundSetPropertyInfo>& getOnCreateSetRelInfos() const {
        return onCreateSetRelInfos;
    }

    void addOnMatchSetNodeInfo(binder::BoundSetPropertyInfo info) {
        onMatchSetNodeInfos.push_back(std::move(info));
    }
    const std::vector<binder::BoundSetPropertyInfo>& getOnMatchSetNodeInfos() const {
        return onMatchSetNodeInfos;
    }

    void addOnMatchSetRelInfo(binder::BoundSetPropertyInfo info) {
        onMatchSetRelInfos.push_back(std::move(info));
    }
    const std::vector<binder::BoundSetPropertyInfo>& getOnMatchSetRelInfos() const {
        return onMatchSetRelInfos;
    }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    std::shared_ptr<binder::Expression> existenceMark;
    std::shared_ptr<binder::Expression> distinctMark;
    // Create infos
    std::vector<LogicalInsertInfo> insertNodeInfos;
    std::vector<LogicalInsertInfo> insertRelInfos;
    // On Create infos
    std::vector<binder::BoundSetPropertyInfo> onCreateSetNodeInfos;
    std::vector<binder::BoundSetPropertyInfo> onCreateSetRelInfos;
    // On Match infos
    std::vector<binder::BoundSetPropertyInfo> onMatchSetNodeInfos;
    std::vector<binder::BoundSetPropertyInfo> onMatchSetRelInfos;
};

} // namespace planner
} // namespace kuzu
