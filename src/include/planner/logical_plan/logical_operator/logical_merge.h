#pragma once

#include "base_logical_operator.h"
#include "logical_create.h"

namespace kuzu {
namespace planner {

class LogicalMerge : public LogicalOperator {
public:
    LogicalMerge(std::shared_ptr<binder::Expression> mark,
        std::vector<std::unique_ptr<LogicalCreateNodeInfo>> createNodeInfos,
        std::vector<std::unique_ptr<LogicalCreateRelInfo>> createRelInfos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::MERGE, std::move(child)}, mark{std::move(mark)},
          createNodeInfos{std::move(createNodeInfos)}, createRelInfos{std::move(createRelInfos)} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const final {
        return std::string(""); // TODO: FIXME
    }

    f_group_pos_set getGroupsPosToFlatten();

    inline std::shared_ptr<binder::Expression> getMark() const { return mark; }
    inline const std::vector<std::unique_ptr<LogicalCreateNodeInfo>>&
    getCreateNodeInfosRef() const {
        return createNodeInfos;
    }
    inline const std::vector<std::unique_ptr<LogicalCreateRelInfo>>& getCreateRelInfosRef() {
        return createRelInfos;
    }

    std::unique_ptr<LogicalOperator> copy() final;

private:
    std::shared_ptr<binder::Expression> mark;
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> createNodeInfos;
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> createRelInfos;
};

} // namespace planner
} // namespace kuzu
