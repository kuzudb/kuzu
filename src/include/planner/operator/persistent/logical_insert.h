#pragma once

#include "binder/expression/rel_expression.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalInsertNodeInfo {
    std::shared_ptr<binder::NodeExpression> node;
    std::vector<binder::expression_pair> setItems;
    binder::expression_vector propertiesToReturn;

    LogicalInsertNodeInfo(std::shared_ptr<binder::NodeExpression> node,
        std::vector<binder::expression_pair> setItems, binder::expression_vector propertiesToReturn)
        : node{std::move(node)}, setItems{std::move(setItems)}, propertiesToReturn{std::move(
                                                                    propertiesToReturn)} {}
    LogicalInsertNodeInfo(const LogicalInsertNodeInfo& other)
        : node{other.node}, setItems{other.setItems}, propertiesToReturn{other.propertiesToReturn} {
    }

    inline std::unique_ptr<LogicalInsertNodeInfo> copy() const {
        return std::make_unique<LogicalInsertNodeInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalInsertNodeInfo>> copy(
        const std::vector<std::unique_ptr<LogicalInsertNodeInfo>>& infos);
};

struct LogicalInsertRelInfo {
    std::shared_ptr<binder::RelExpression> rel;
    std::vector<binder::expression_pair> setItems;
    binder::expression_vector propertiesToReturn;

    LogicalInsertRelInfo(std::shared_ptr<binder::RelExpression> rel,
        std::vector<binder::expression_pair> setItems, binder::expression_vector propertiesToReturn)
        : rel{std::move(rel)}, setItems{std::move(setItems)}, propertiesToReturn{
                                                                  std::move(propertiesToReturn)} {}
    LogicalInsertRelInfo(const LogicalInsertRelInfo& other)
        : rel{other.rel}, setItems{other.setItems}, propertiesToReturn{other.propertiesToReturn} {}

    inline std::unique_ptr<LogicalInsertRelInfo> copy() const {
        return std::make_unique<LogicalInsertRelInfo>(*this);
    }

    static std::vector<std::unique_ptr<LogicalInsertRelInfo>> copy(
        const std::vector<std::unique_ptr<LogicalInsertRelInfo>>& infos);
};

class LogicalInsertNode : public LogicalOperator {
public:
    LogicalInsertNode(std::vector<std::unique_ptr<LogicalInsertNodeInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::INSERT_NODE, std::move(child)}, infos{std::move(
                                                                                   infos)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    f_group_pos_set getGroupsPosToFlatten();

    inline const std::vector<std::unique_ptr<LogicalInsertNodeInfo>>& getInfosRef() const {
        return infos;
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalInsertNode>(
            LogicalInsertNodeInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalInsertNodeInfo>> infos;
};

class LogicalInsertRel : public LogicalOperator {
public:
    LogicalInsertRel(std::vector<std::unique_ptr<LogicalInsertRelInfo>> infos,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::INSERT_REL, std::move(child)}, infos{std::move(
                                                                                  infos)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const final;

    f_group_pos_set getGroupsPosToFlatten();

    inline const std::vector<std::unique_ptr<LogicalInsertRelInfo>>& getInfosRef() { return infos; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalInsertRel>(
            LogicalInsertRelInfo::copy(infos), children[0]->copy());
    }

private:
    std::vector<std::unique_ptr<LogicalInsertRelInfo>> infos;
};

} // namespace planner
} // namespace kuzu
