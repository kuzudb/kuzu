#pragma once

#include "planner/operator/logical_operator.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/operator/persistent/logical_set.h"

namespace kuzu {
namespace planner {

class LogicalMerge : public LogicalOperator {
public:
    LogicalMerge(std::shared_ptr<binder::Expression> mark,
        std::vector<std::unique_ptr<LogicalInsertNodeInfo>> insertNodeInfos,
        std::vector<std::unique_ptr<LogicalInsertRelInfo>> insertRelInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetNodeInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetRelInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetNodeInfos,
        std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetRelInfos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::MERGE, std::move(child)}, mark{std::move(mark)},
          insertNodeInfos{std::move(insertNodeInfos)}, insertRelInfos{std::move(insertRelInfos)},
          onCreateSetNodeInfos{std::move(onCreateSetNodeInfos)},
          onCreateSetRelInfos(std::move(onCreateSetRelInfos)),
          onMatchSetNodeInfos{std::move(onMatchSetNodeInfos)}, onMatchSetRelInfos{
                                                                   std::move(onMatchSetRelInfos)} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const final { return std::string(""); }

    f_group_pos_set getGroupsPosToFlatten();

    inline std::shared_ptr<binder::Expression> getMark() const { return mark; }
    inline const std::vector<std::unique_ptr<LogicalInsertNodeInfo>>&
    getInsertNodeInfosRef() const {
        return insertNodeInfos;
    }
    inline const std::vector<std::unique_ptr<LogicalInsertRelInfo>>& getInsertRelInfosRef() const {
        return insertRelInfos;
    }
    inline const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>&
    getOnCreateSetNodeInfosRef() const {
        return onCreateSetNodeInfos;
    }
    inline const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>&
    getOnCreateSetRelInfosRef() const {
        return onCreateSetRelInfos;
    }
    inline const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>&
    getOnMatchSetNodeInfosRef() const {
        return onMatchSetNodeInfos;
    }
    inline const std::vector<std::unique_ptr<LogicalSetPropertyInfo>>&
    getOnMatchSetRelInfosRef() const {
        return onMatchSetRelInfos;
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalMerge>(mark, LogicalInsertNodeInfo::copy(insertNodeInfos),
            LogicalInsertRelInfo::copy(insertRelInfos),
            LogicalSetPropertyInfo::copy(onCreateSetNodeInfos),
            LogicalSetPropertyInfo::copy(onCreateSetRelInfos),
            LogicalSetPropertyInfo::copy(onMatchSetNodeInfos),
            LogicalSetPropertyInfo::copy(onMatchSetRelInfos), children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> mark;
    // Create infos
    std::vector<std::unique_ptr<LogicalInsertNodeInfo>> insertNodeInfos;
    std::vector<std::unique_ptr<LogicalInsertRelInfo>> insertRelInfos;
    // On Create infos
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetNodeInfos;
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onCreateSetRelInfos;
    // On Match infos
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetNodeInfos;
    std::vector<std::unique_ptr<LogicalSetPropertyInfo>> onMatchSetRelInfos;
};

} // namespace planner
} // namespace kuzu
