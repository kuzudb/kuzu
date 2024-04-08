#pragma once

#include "planner/operator/logical_operator.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/operator/persistent/logical_set.h"

namespace kuzu {
namespace planner {

class LogicalMerge : public LogicalOperator {
public:
    LogicalMerge(std::shared_ptr<binder::Expression> existenceMark,
        std::shared_ptr<binder::Expression> distinctMark,
        std::vector<LogicalInsertInfo> insertNodeInfos,
        std::vector<LogicalInsertInfo> insertRelInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetNodeInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetRelInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetNodeInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetRelInfos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::MERGE, std::move(child)},
          existenceMark{std::move(existenceMark)}, distinctMark{std::move(distinctMark)},
          insertNodeInfos{std::move(insertNodeInfos)}, insertRelInfos{std::move(insertRelInfos)},
          onCreateSetNodeInfos{std::move(onCreateSetNodeInfos)},
          onCreateSetRelInfos(std::move(onCreateSetRelInfos)),
          onMatchSetNodeInfos{std::move(onMatchSetNodeInfos)},
          onMatchSetRelInfos{std::move(onMatchSetRelInfos)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final { return {}; }

    f_group_pos_set getGroupsPosToFlatten();

    std::shared_ptr<binder::Expression> getExistenceMark() const { return existenceMark; }
    bool hasDistinctMark() const { return distinctMark != nullptr; }
    std::shared_ptr<binder::Expression> getDistinctMark() const { return distinctMark; }

    const std::vector<LogicalInsertInfo>& getInsertNodeInfosRef() const { return insertNodeInfos; }
    const std::vector<LogicalInsertInfo>& getInsertRelInfosRef() const { return insertRelInfos; }
    const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>& getOnCreateSetNodeInfosRef() const {
        return onCreateSetNodeInfos;
    }
    const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>& getOnCreateSetRelInfosRef() const {
        return onCreateSetRelInfos;
    }
    const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>& getOnMatchSetNodeInfosRef() const {
        return onMatchSetNodeInfos;
    }
    const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>& getOnMatchSetRelInfosRef() const {
        return onMatchSetRelInfos;
    }

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalMerge>(existenceMark, distinctMark,
            copyVector(insertNodeInfos), copyVector(insertRelInfos),
            LogicalSetPropertyInfo::copy(onCreateSetNodeInfos),
            LogicalSetPropertyInfo::copy(onCreateSetRelInfos),
            LogicalSetPropertyInfo::copy(onMatchSetNodeInfos),
            LogicalSetPropertyInfo::copy(onMatchSetRelInfos), children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> existenceMark;
    std::shared_ptr<binder::Expression> distinctMark;
    // Create infos
    std::vector<LogicalInsertInfo> insertNodeInfos;
    std::vector<LogicalInsertInfo> insertRelInfos;
    // On Create infos
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetNodeInfos;
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetRelInfos;
    // On Match infos
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetNodeInfos;
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetRelInfos;
};

} // namespace planner
} // namespace kuzu
