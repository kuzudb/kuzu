#pragma once

#include "base_logical_operator.h"
#include "binder/expression/rel_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalExtend : public LogicalOperator {
public:
    LogicalExtend(shared_ptr<NodeExpression> boundNode, shared_ptr<NodeExpression> nbrNode,
        shared_ptr<RelExpression> rel, RelDirection direction, expression_vector properties,
        bool extendToNewGroup, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::EXTEND, std::move(child)}, boundNode{std::move(
                                                                              boundNode)},
          nbrNode{std::move(nbrNode)}, rel{std::move(rel)}, direction{direction},
          properties{std::move(properties)}, extendToNewGroup{extendToNewGroup} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override {
        return boundNode->getRawName() + (direction == RelDirection::FWD ? "->" : "<-") +
               nbrNode->getRawName();
    }

    inline shared_ptr<NodeExpression> getBoundNode() const { return boundNode; }
    inline shared_ptr<NodeExpression> getNbrNode() const { return nbrNode; }
    inline shared_ptr<RelExpression> getRel() const { return rel; }
    inline RelDirection getDirection() const { return direction; }
    inline expression_vector getProperties() const { return properties; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(
            boundNode, nbrNode, rel, direction, properties, extendToNewGroup, children[0]->copy());
    }

protected:
    shared_ptr<NodeExpression> boundNode;
    shared_ptr<NodeExpression> nbrNode;
    shared_ptr<RelExpression> rel;
    RelDirection direction;
    expression_vector properties;
    // When extend might increase cardinality (i.e. n * m), we extend to a new factorization group.
    bool extendToNewGroup;
};

} // namespace planner
} // namespace kuzu
