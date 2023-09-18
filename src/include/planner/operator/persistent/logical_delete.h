#pragma once

#include "binder/expression/rel_expression.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDeleteNode : public LogicalOperator {
public:
    LogicalDeleteNode(std::vector<std::shared_ptr<binder::NodeExpression>> nodes,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DELETE_NODE, std::move(child)}, nodes{std::move(
                                                                                   nodes)} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const final;

    inline const std::vector<std::shared_ptr<binder::NodeExpression>>& getNodesRef() const {
        return nodes;
    }

    f_group_pos_set getGroupsPosToFlatten();

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalDeleteNode>(nodes, children[0]->copy());
    }

private:
    std::vector<std::shared_ptr<binder::NodeExpression>> nodes;
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
