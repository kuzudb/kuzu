#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {

struct LogicalCreateNodeInfo {
    std::shared_ptr<binder::NodeExpression> node;
    std::shared_ptr<binder::Expression> primaryKey;

    LogicalCreateNodeInfo(std::shared_ptr<binder::NodeExpression> node,
        std::shared_ptr<binder::Expression> primaryKey)
        : node{std::move(node)}, primaryKey{std::move(primaryKey)} {}
    LogicalCreateNodeInfo(const LogicalCreateNodeInfo& other)
        : node{other.node}, primaryKey{other.primaryKey} {}

    inline std::unique_ptr<LogicalCreateNodeInfo> copy() const {
        return std::make_unique<LogicalCreateNodeInfo>(*this);
    }
};

struct LogicalCreateRelInfo {
    std::shared_ptr<binder::RelExpression> rel;
    std::vector<binder::expression_pair> setItems;

    LogicalCreateRelInfo(
        std::shared_ptr<binder::RelExpression> rel, std::vector<binder::expression_pair> setItems)
        : rel{std::move(rel)}, setItems{std::move(setItems)} {}
    LogicalCreateRelInfo(const LogicalCreateRelInfo& other)
        : rel{other.rel}, setItems{other.setItems} {}

    inline std::unique_ptr<LogicalCreateRelInfo> copy() const {
        return std::make_unique<LogicalCreateRelInfo>(*this);
    }
};

class LogicalCreateNode : public LogicalOperator {
public:
    LogicalCreateNode(std::vector<std::unique_ptr<LogicalCreateNodeInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::CREATE_NODE, std::move(child)}, infos{std::move(
                                                                                   infos)} {}
    ~LogicalCreateNode() override = default;

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const override {
        return std::string(""); // TODO: FIXME
    }

    f_group_pos_set getGroupsPosToFlatten();

    inline const std::vector<std::unique_ptr<LogicalCreateNodeInfo>>& getInfosRef() const {
        return infos;
    }

    std::unique_ptr<LogicalOperator> copy() final;

private:
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> infos;
};

class LogicalCreateRel : public LogicalOperator {
public:
    LogicalCreateRel(std::vector<std::unique_ptr<LogicalCreateRelInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::CREATE_REL, std::move(child)}, infos{std::move(
                                                                                  infos)} {}
    ~LogicalCreateRel() override = default;

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const final {
        return std::string(""); // TODO: FIXME
    }

    f_group_pos_set getGroupsPosToFlatten();

    inline const std::vector<std::unique_ptr<LogicalCreateRelInfo>>& getInfosRef() { return infos; }

    std::unique_ptr<LogicalOperator> copy() final;

private:
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> infos;
};

} // namespace planner
} // namespace kuzu
