#pragma once

#include "binder/expression/rel_expression.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundCreateNodeOrRel {
public:
    BoundCreateNodeOrRel(vector<expression_pair> setItems) : setItems{std::move(setItems)} {}
    virtual ~BoundCreateNodeOrRel() = default;

    inline vector<expression_pair> getSetItems() const { return setItems; }

protected:
    vector<expression_pair> setItems;
};

class BoundCreateNode : public BoundCreateNodeOrRel {
public:
    BoundCreateNode(shared_ptr<NodeExpression> node, shared_ptr<Expression> primaryKeyExpression,
        vector<expression_pair> setItems)
        : BoundCreateNodeOrRel{std::move(setItems)}, node{std::move(node)},
          primaryKeyExpression{std::move(primaryKeyExpression)} {}

    inline shared_ptr<NodeExpression> getNode() const { return node; }
    inline shared_ptr<Expression> getPrimaryKeyExpression() const { return primaryKeyExpression; }

    inline unique_ptr<BoundCreateNode> copy() const {
        return make_unique<BoundCreateNode>(node, primaryKeyExpression, setItems);
    }

private:
    shared_ptr<NodeExpression> node;
    shared_ptr<Expression> primaryKeyExpression;
};

class BoundCreateRel : public BoundCreateNodeOrRel {
public:
    BoundCreateRel(shared_ptr<RelExpression> rel, vector<expression_pair> setItems)
        : BoundCreateNodeOrRel{std::move(setItems)}, rel{std::move(rel)} {}

    inline shared_ptr<RelExpression> getRel() const { return rel; }

    inline unique_ptr<BoundCreateRel> copy() const {
        return make_unique<BoundCreateRel>(rel, setItems);
    }

private:
    shared_ptr<RelExpression> rel;
};

class BoundCreateClause : public BoundUpdatingClause {
public:
    BoundCreateClause() : BoundUpdatingClause{ClauseType::CREATE} {};
    ~BoundCreateClause() override = default;

    inline void addCreateNode(unique_ptr<BoundCreateNode> createNode) {
        createNodes.push_back(std::move(createNode));
    }
    inline bool hasCreateNode() const { return !createNodes.empty(); }
    inline uint32_t getNumCreateNodes() const { return createNodes.size(); }
    inline BoundCreateNode* getCreateNode(uint32_t idx) const { return createNodes[idx].get(); }
    inline const vector<unique_ptr<BoundCreateNode>>& getCreateNodes() const { return createNodes; }

    inline void addCreateRel(unique_ptr<BoundCreateRel> createRel) {
        createRels.push_back(std::move(createRel));
    }
    inline bool hasCreateRel() const { return !createRels.empty(); }
    inline uint32_t getNumCreateRels() const { return createRels.size(); }
    inline BoundCreateRel* getCreateRel(uint32_t idx) const { return createRels[idx].get(); }
    inline const vector<unique_ptr<BoundCreateRel>>& getCreateRels() const { return createRels; }

    vector<expression_pair> getAllSetItems() const;
    expression_vector getPropertiesToRead() const override;

    unique_ptr<BoundUpdatingClause> copy() override;

private:
    vector<unique_ptr<BoundCreateNode>> createNodes;
    vector<unique_ptr<BoundCreateRel>> createRels;
};

} // namespace binder
} // namespace kuzu
