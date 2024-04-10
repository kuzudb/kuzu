#pragma once

#include "planner/operator/extend/base_logical_extend.h"

namespace kuzu {
namespace planner {

class LogicalExtend : public BaseLogicalExtend {
public:
    LogicalExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        ExtendDirection direction, binder::expression_vector properties, bool hasAtMostOneNbr,
        std::shared_ptr<LogicalOperator> child)
        : BaseLogicalExtend{LogicalOperatorType::EXTEND, std::move(boundNode), std::move(nbrNode),
              std::move(rel), direction, std::move(child)},
          properties{std::move(properties)}, hasAtMostOneNbr{hasAtMostOneNbr} {}

    f_group_pos_set getGroupsPosToFlatten() override;

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline binder::expression_vector getProperties() const { return properties; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(boundNode, nbrNode, rel, direction, properties,
            hasAtMostOneNbr, children[0]->copy());
    }

private:
    binder::expression_vector properties;
    bool hasAtMostOneNbr;
};

} // namespace planner
} // namespace kuzu
