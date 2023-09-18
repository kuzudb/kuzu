#pragma once

#include "binder/expression/rel_expression.h"
#include "planner/operator/extend/extend_direction.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class BaseLogicalExtend : public LogicalOperator {
public:
    BaseLogicalExtend(LogicalOperatorType operatorType,
        std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        ExtendDirection direction, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType, std::move(child)}, boundNode{std::move(boundNode)},
          nbrNode{std::move(nbrNode)}, rel{std::move(rel)}, direction{direction} {}

    inline std::shared_ptr<binder::NodeExpression> getBoundNode() const { return boundNode; }
    inline std::shared_ptr<binder::NodeExpression> getNbrNode() const { return nbrNode; }
    inline std::shared_ptr<binder::RelExpression> getRel() const { return rel; }
    inline ExtendDirection getDirection() const { return direction; }
    virtual f_group_pos_set getGroupsPosToFlatten() = 0;

    std::string getExpressionsForPrinting() const override;

protected:
    std::shared_ptr<binder::NodeExpression> boundNode;
    std::shared_ptr<binder::NodeExpression> nbrNode;
    std::shared_ptr<binder::RelExpression> rel;
    ExtendDirection direction;
};

} // namespace planner
} // namespace kuzu
