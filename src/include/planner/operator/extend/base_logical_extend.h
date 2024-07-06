#pragma once

#include "binder/expression/rel_expression.h"
#include "common/enums/extend_direction.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class BaseLogicalExtend : public LogicalOperator {
public:
    BaseLogicalExtend(LogicalOperatorType operatorType,
        std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::ExtendDirection direction, bool extendFromSource_,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType, std::move(child)}, boundNode{std::move(boundNode)},
          nbrNode{std::move(nbrNode)}, rel{std::move(rel)}, direction{direction},
          extendFromSource_{extendFromSource_} {}

    std::shared_ptr<binder::NodeExpression> getBoundNode() const { return boundNode; }
    std::shared_ptr<binder::NodeExpression> getNbrNode() const { return nbrNode; }
    std::shared_ptr<binder::RelExpression> getRel() const { return rel; }
    bool isRecursive() const { return rel->isRecursive(); }
    common::ExtendDirection getDirection() const { return direction; }

    bool extendFromSourceNode() const { return *boundNode == *rel->getSrcNode(); }

    virtual f_group_pos_set getGroupsPosToFlatten() = 0;

    std::string getExpressionsForPrinting() const override;

protected:
    // Start node of extension.
    std::shared_ptr<binder::NodeExpression> boundNode;
    // End node of extension.
    std::shared_ptr<binder::NodeExpression> nbrNode;
    std::shared_ptr<binder::RelExpression> rel;
    common::ExtendDirection direction;
    // Ideally we should check this by *boundNode == *rel->getSrcNode()
    // This is currently not doable due to recursive plan not setting src node correctly.
    bool extendFromSource_;
};

} // namespace planner
} // namespace kuzu
