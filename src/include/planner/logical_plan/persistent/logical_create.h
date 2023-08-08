#pragma once

#include "binder/expression/rel_expression.h"
#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalCreateNodeInfo {
    std::shared_ptr<binder::NodeExpression> node;
    std::vector<binder::expression_pair> setItems;
    binder::expression_vector propertiesToReturn;

    LogicalCreateNodeInfo(std::shared_ptr<binder::NodeExpression> node,
        std::vector<binder::expression_pair> setItems, binder::expression_vector propertiesToReturn)
        : node{std::move(node)}, setItems{std::move(setItems)}, propertiesToReturn{std::move(
                                                                    propertiesToReturn)} {}
    LogicalCreateNodeInfo(const LogicalCreateNodeInfo& other)
        : node{other.node}, setItems{other.setItems}, propertiesToReturn{other.propertiesToReturn} {
    }

    inline std::unique_ptr<LogicalCreateNodeInfo> copy() const {
        return std::make_unique<LogicalCreateNodeInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalCreateNodeInfo>> copy(
        const std::vector<std::unique_ptr<LogicalCreateNodeInfo>>& infos);
};

struct LogicalCreateRelInfo {
    std::shared_ptr<binder::RelExpression> rel;
    std::vector<binder::expression_pair> setItems;
    binder::expression_vector propertiesToReturn;

    LogicalCreateRelInfo(std::shared_ptr<binder::RelExpression> rel,
        std::vector<binder::expression_pair> setItems, binder::expression_vector propertiesToReturn)
        : rel{std::move(rel)}, setItems{std::move(setItems)}, propertiesToReturn{
                                                                  std::move(propertiesToReturn)} {}
    LogicalCreateRelInfo(const LogicalCreateRelInfo& other)
        : rel{other.rel}, setItems{other.setItems}, propertiesToReturn{other.propertiesToReturn} {}

    inline std::unique_ptr<LogicalCreateRelInfo> copy() const {
        return std::make_unique<LogicalCreateRelInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalCreateRelInfo>> copy(
        const std::vector<std::unique_ptr<LogicalCreateRelInfo>>& infos);
};

class LogicalCreateNode : public LogicalOperator {
public:
    LogicalCreateNode(std::vector<std::unique_ptr<LogicalCreateNodeInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::CREATE_NODE, std::move(child)}, infos{std::move(
                                                                                   infos)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    f_group_pos_set getGroupsPosToFlatten();

    inline const std::vector<std::unique_ptr<LogicalCreateNodeInfo>>& getInfosRef() const {
        return infos;
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateNode>(
            LogicalCreateNodeInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalCreateNodeInfo>> infos;
};

class LogicalCreateRel : public LogicalOperator {
public:
    LogicalCreateRel(std::vector<std::unique_ptr<LogicalCreateRelInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::CREATE_REL, std::move(child)}, infos{std::move(
                                                                                  infos)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    f_group_pos_set getGroupsPosToFlatten();

    inline const std::vector<std::unique_ptr<LogicalCreateRelInfo>>& getInfosRef() { return infos; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalCreateRel>(
            LogicalCreateRelInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalCreateRelInfo>> infos;
};

} // namespace planner
} // namespace kuzu
