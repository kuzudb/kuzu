#pragma once

#include "binder/query/reading_clause/bound_match_clause.h"
#include "join_tree.h"

namespace kuzu {
namespace planner {

class PropertyExprCollection;

class JoinTreeConstructor {
public:
    JoinTreeConstructor(const binder::QueryGraph& queryGraph,
        const PropertyExprCollection& propertyCollection, binder::expression_vector predicates)
        : queryGraph{queryGraph}, propertyCollection{propertyCollection},
          queryGraphPredicates{std::move(predicates)} {}

    JoinTree construct(std::shared_ptr<binder::BoundJoinHintNode> root);

private:
    struct IntermediateResult {
        std::shared_ptr<JoinTreeNode> treeNode = nullptr;
        binder::SubqueryGraph subqueryGraph;
    };

    IntermediateResult constructTreeNode(std::shared_ptr<binder::BoundJoinHintNode> hintNode);
    IntermediateResult constructNodeScan(std::shared_ptr<binder::Expression> expr);
    IntermediateResult constructRelScan(std::shared_ptr<binder::Expression> expr);

    std::shared_ptr<JoinTreeNode> tryConstructNestedLoopJoin(
        std::vector<std::shared_ptr<binder::NodeExpression>> joinNodes,
        const JoinTreeNode& leftRoot, const JoinTreeNode& rightRoot,
        const binder::expression_vector& predicates);

private:
    const binder::QueryGraph& queryGraph;
    const PropertyExprCollection& propertyCollection;
    binder::expression_vector queryGraphPredicates;
};

} // namespace planner
} // namespace kuzu
