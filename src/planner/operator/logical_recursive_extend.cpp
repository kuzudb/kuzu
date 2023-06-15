#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"

#include "optimizer/factorization_rewriter.h"
#include "optimizer/remove_factorization_rewriter.h"
#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalRecursiveExtend::getGroupsPosToFlatten() {
    f_group_pos_set result;
    auto inSchema = children[0]->getSchema();
    auto boundNodeGroupPos = inSchema->getGroupPos(*boundNode->getInternalIDProperty());
    if (!inSchema->getGroup(boundNodeGroupPos)->isFlat()) {
        result.insert(boundNodeGroupPos);
    }
    return result;
}

void LogicalRecursiveExtend::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(nbrNode->getInternalIDProperty(), 0);
    schema->insertToGroupAndScope(rel->getLengthExpression(), 0);
    switch (joinType) {
    case RecursiveJoinType::TRACK_PATH: {
        schema->insertToGroupAndScope(rel, 0);
    } break;
    default:
        break;
    }
    auto rewriter = optimizer::RemoveFactorizationRewriter();
    rewriter.visitOperator(recursiveChild);
}

void LogicalRecursiveExtend::computeFactorizedSchema() {
    copyChildSchema(0);
    auto nbrGroupPos = schema->createGroup();
    schema->insertToGroupAndScope(nbrNode->getInternalIDProperty(), nbrGroupPos);
    schema->insertToGroupAndScope(rel->getLengthExpression(), nbrGroupPos);
    switch (joinType) {
    case RecursiveJoinType::TRACK_PATH: {
        schema->insertToGroupAndScope(rel, nbrGroupPos);
    } break;
    default:
        break;
    }
    auto rewriter = optimizer::FactorizationRewriter();
    rewriter.visitOperator(recursiveChild.get());
}

void LogicalScanFrontier::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->setGroupAsSingleState(0);
    schema->insertToGroupAndScope(node->getInternalIDProperty(), 0);
}

void LogicalScanFrontier::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->setGroupAsSingleState(groupPos);
    schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
}

} // namespace planner
} // namespace kuzu
