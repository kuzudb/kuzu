#include "planner/operator/extend/logical_extend.h"

namespace kuzu {
namespace planner {

void LogicalExtend::computeFactorizedSchema() {
    copyChildSchema(0);
    uint32_t nbrGroupPos = 0u;
    nbrGroupPos = schema->createGroup();
    schema->insertToGroupAndScope(nbrNode->getInternalID(), nbrGroupPos);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, nbrGroupPos);
    }
    if (rel->hasDirectionExpr()) {
        schema->insertToGroupAndScope(rel->getDirectionExpr(), nbrGroupPos);
    }
}

void LogicalExtend::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(nbrNode->getInternalID(), 0);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, 0);
    }
    if (rel->hasDirectionExpr()) {
        schema->insertToGroupAndScope(rel->getDirectionExpr(), 0);
    }
}

std::unique_ptr<LogicalOperator> LogicalExtend::copy() {
    auto extend = std::make_unique<LogicalExtend>(boundNode, nbrNode, rel, direction,
        extendFromSource_, properties, children[0]->copy());
    extend->setPropertyPredicates(copyVector(propertyPredicates));
    return extend;
}

} // namespace planner
} // namespace kuzu
