#pragma once

#include "binder/expression/rel_expression.h"
#include "common/enums/extend_direction.h"
#include "planner/operator/logical_operator.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

class LogicalCSRBuild : public LogicalOperator {
public:
    LogicalCSRBuild(LogicalOperatorType operatorType,
        std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        ExtendDirection direction, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType, std::move(child)}, boundNode{std::move(boundNode)},
          nbrNode{std::move(nbrNode)}, rel{std::move(rel)}, direction{direction} {}

    inline std::shared_ptr<binder::NodeExpression> getBoundNode() const { return boundNode; }
    inline std::shared_ptr<binder::NodeExpression> getNbrNode() const { return nbrNode; }
    inline std::shared_ptr<binder::RelExpression> getRel() const { return rel; }
    inline ExtendDirection getDirection() const { return direction; }
    virtual f_group_pos_set getGroupsPosToFlatten();

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalCSRBuild>(
            operatorType, boundNode, nbrNode, rel, direction, children[0]->copy());
    }

private:
    std::shared_ptr<binder::NodeExpression> boundNode;
    std::shared_ptr<binder::NodeExpression> nbrNode;
    std::shared_ptr<binder::RelExpression> rel;
    ExtendDirection direction;
};

} // namespace planner
} // namespace kuzu
