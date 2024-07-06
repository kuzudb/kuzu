#include "planner/operator/extend/logical_extend.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalExtend::getGroupsPosToFlatten() {
    f_group_pos_set result;
    auto inSchema = children[0]->getSchema();
    auto boundNodeGroupPos = inSchema->getGroupPos(*boundNode->getInternalID());
    if (!inSchema->getGroup(boundNodeGroupPos)->isFlat()) {
        result.insert(boundNodeGroupPos);
    }
    return result;
}

void LogicalExtend::computeFactorizedSchema() {
    copyChildSchema(0);
    RUNTIME_CHECK({
        auto boundGroupPos = schema->getGroupPos(*boundNode->getInternalID());
        KU_ASSERT(schema->getGroup(boundGroupPos)->isFlat());
    });
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
        extendFromSource_, properties, hasAtMostOneNbr, children[0]->copy());
    extend->setPropertyPredicates(copyVector(propertyPredicates));
    return extend;
}

} // namespace planner
} // namespace kuzu
