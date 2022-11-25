#pragma once

#include "binder/expression/rel_expression.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundDeleteNode {
public:
    BoundDeleteNode(shared_ptr<NodeExpression> node, shared_ptr<Expression> primaryKeyExpression)
        : node{std::move(node)}, primaryKeyExpression{std::move(primaryKeyExpression)} {}

    inline shared_ptr<NodeExpression> getNode() const { return node; }
    inline shared_ptr<Expression> getPrimaryKeyExpression() const { return primaryKeyExpression; }

    inline unique_ptr<BoundDeleteNode> copy() {
        return make_unique<BoundDeleteNode>(node, primaryKeyExpression);
    }

private:
    shared_ptr<NodeExpression> node;
    shared_ptr<Expression> primaryKeyExpression;
};

class BoundDeleteClause : public BoundUpdatingClause {
public:
    BoundDeleteClause() : BoundUpdatingClause{ClauseType::DELETE} {};
    ~BoundDeleteClause() override = default;

    inline void addDeleteNode(unique_ptr<BoundDeleteNode> deleteNode) {
        deleteNodes.push_back(std::move(deleteNode));
    }
    inline bool hasDeleteNode() const { return !deleteNodes.empty(); }
    inline uint32_t getNumDeleteNodes() const { return deleteNodes.size(); }
    inline BoundDeleteNode* getDeleteNode(uint32_t idx) const { return deleteNodes[idx].get(); }
    inline const vector<unique_ptr<BoundDeleteNode>>& getDeleteNodes() const { return deleteNodes; }

    inline void addDeleteRel(shared_ptr<RelExpression> deleteRel) {
        deleteRels.push_back(std::move(deleteRel));
    }
    inline bool hasDeleteRel() const { return !deleteRels.empty(); }
    inline uint32_t getNumDeleteRels() const { return deleteRels.size(); }
    inline shared_ptr<RelExpression> getDeleteRel(uint32_t idx) const { return deleteRels[idx]; }
    inline vector<shared_ptr<RelExpression>> getDeleteRels() const { return deleteRels; }

    expression_vector getPropertiesToRead() const override;

    unique_ptr<BoundUpdatingClause> copy() override;

private:
    vector<unique_ptr<BoundDeleteNode>> deleteNodes;
    vector<shared_ptr<RelExpression>> deleteRels;
};

} // namespace binder
} // namespace kuzu
