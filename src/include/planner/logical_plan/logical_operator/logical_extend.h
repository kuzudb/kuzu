#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {

class LogicalExtend : public LogicalOperator {
public:
    LogicalExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::RelDirection direction, binder::expression_vector properties, bool extendToNewGroup,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::EXTEND, std::move(child)}, boundNode{std::move(
                                                                              boundNode)},
          nbrNode{std::move(nbrNode)}, rel{std::move(rel)}, direction{direction},
          properties{std::move(properties)}, extendToNewGroup{extendToNewGroup} {}

    f_group_pos_set getGroupsPosToFlatten();

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return boundNode->toString() + (direction == common::RelDirection::FWD ? "->" : "<-") +
               nbrNode->toString();
    }

    inline std::shared_ptr<binder::NodeExpression> getBoundNode() const { return boundNode; }
    inline std::shared_ptr<binder::NodeExpression> getNbrNode() const { return nbrNode; }
    inline std::shared_ptr<binder::RelExpression> getRel() const { return rel; }
    inline common::RelDirection getDirection() const { return direction; }
    inline binder::expression_vector getProperties() const { return properties; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(
            boundNode, nbrNode, rel, direction, properties, extendToNewGroup, children[0]->copy());
    }

protected:
    std::shared_ptr<binder::NodeExpression> boundNode;
    std::shared_ptr<binder::NodeExpression> nbrNode;
    std::shared_ptr<binder::RelExpression> rel;
    common::RelDirection direction;
    binder::expression_vector properties;
    // When extend might increase cardinality (i.e. n * m), we extend to a new factorization group.
    bool extendToNewGroup;
};

} // namespace planner
} // namespace kuzu
