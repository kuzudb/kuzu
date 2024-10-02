#include "planner/operator/extend/logical_recursive_extend.h"

#include "optimizer/factorization_rewriter.h"
#include "optimizer/remove_factorization_rewriter.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalRecursiveExtend::getGroupsPosToFlatten() {
    f_group_pos_set result;
    auto inSchema = children[0]->getSchema();
    auto boundNodeGroupPos = inSchema->getGroupPos(*boundNode->getInternalID());
    if (!inSchema->getGroup(boundNodeGroupPos)->isFlat()) {
        result.insert(boundNodeGroupPos);
    }
    return result;
}

void LogicalRecursiveExtend::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(nbrNode->getInternalID(), 0);
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
    schema->insertToGroupAndScope(nbrNode->getInternalID(), nbrGroupPos);
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

void LogicalPathPropertyProbe::computeFactorizedSchema() {
    copyChildSchema(0);
    if (pathNodeIDs != nullptr) {
        KU_ASSERT(schema->getNumGroups() == 1);
        schema->insertToGroupAndScope(recursiveRel, 0);
    }

    if (nodeChild != nullptr) {
        auto rewriter = optimizer::FactorizationRewriter();
        rewriter.visitOperator(nodeChild.get());
    }
    if (relChild != nullptr) {
        auto rewriter = optimizer::FactorizationRewriter();
        rewriter.visitOperator(relChild.get());
    }
}

void LogicalPathPropertyProbe::computeFlatSchema() {
    copyChildSchema(0);
    if (pathNodeIDs != nullptr) {
        KU_ASSERT(schema->getNumGroups() == 1);
        schema->insertToGroupAndScope(recursiveRel, 0);
    }

    if (nodeChild != nullptr) {
        auto rewriter = optimizer::RemoveFactorizationRewriter();
        rewriter.visitOperator(nodeChild);
    }
    if (relChild != nullptr) {
        auto rewriter = optimizer::RemoveFactorizationRewriter();
        rewriter.visitOperator(relChild);
    }
}

std::unique_ptr<LogicalOperator> LogicalPathPropertyProbe::copy() {
    auto nodeChildCopy = nodeChild == nullptr ? nullptr : nodeChild->copy();
    auto relChildCopy = relChild == nullptr ? nullptr : relChild->copy();
    auto op = std::make_unique<LogicalPathPropertyProbe>(recursiveRel, children[0]->copy(),
        std::move(nodeChildCopy), std::move(relChildCopy), joinType);
    op->sipInfo = sipInfo;
    op->direction = direction;
    op->extendFromSource_ = extendFromSource_;
    op->pathNodeIDs = pathNodeIDs;
    op->pathEdgeIDs = pathEdgeIDs;
    return op;
}

} // namespace planner
} // namespace kuzu
