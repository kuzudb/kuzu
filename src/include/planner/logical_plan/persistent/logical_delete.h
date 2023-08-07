#pragma once

#include "binder/expression/rel_expression.h"
#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalDeleteNodeInfo {
    std::shared_ptr<binder::NodeExpression> node;
    std::shared_ptr<binder::Expression> primaryKey;

    LogicalDeleteNodeInfo(std::shared_ptr<binder::NodeExpression> node,
        std::shared_ptr<binder::Expression> primaryKey)
        : node{std::move(node)}, primaryKey{std::move(primaryKey)} {}
    LogicalDeleteNodeInfo(const LogicalDeleteNodeInfo& other)
        : node{other.node}, primaryKey{other.primaryKey} {}

    inline std::unique_ptr<LogicalDeleteNodeInfo> copy() const {
        return std::make_unique<LogicalDeleteNodeInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalDeleteNodeInfo>> copy(
        const std::vector<std::unique_ptr<LogicalDeleteNodeInfo>>& infos);
};

class LogicalDeleteNode : public LogicalOperator {
public:
    LogicalDeleteNode(std::vector<std::unique_ptr<LogicalDeleteNodeInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DELETE_NODE, std::move(child)}, infos{std::move(
                                                                                   infos)} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const final;

    inline const std::vector<std::unique_ptr<LogicalDeleteNodeInfo>>& getInfosRef() const {
        return infos;
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalDeleteNode>(
            LogicalDeleteNodeInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalDeleteNodeInfo>> infos;
};

class LogicalDeleteRel : public LogicalOperator {
public:
    LogicalDeleteRel(std::vector<std::shared_ptr<binder::RelExpression>> rels,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DELETE_REL, std::move(child)}, rels{std::move(
                                                                                  rels)} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const final;

    inline const std::vector<std::shared_ptr<binder::RelExpression>>& getRelsRef() const {
        return rels;
    }

    f_group_pos_set getGroupsPosToFlatten(uint32_t idx);

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDeleteRel>(rels, children[0]->copy());
    }

private:
    std::vector<std::shared_ptr<binder::RelExpression>> rels;
};

} // namespace planner
} // namespace kuzu
