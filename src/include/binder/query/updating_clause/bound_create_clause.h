#pragma once

#include "binder/expression/rel_expression.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundCreateNodeOrRel {
public:
    explicit BoundCreateNodeOrRel(std::vector<expression_pair> setItems)
        : setItems{std::move(setItems)} {}
    virtual ~BoundCreateNodeOrRel() = default;

    inline std::vector<expression_pair> getSetItems() const { return setItems; }

protected:
    std::vector<expression_pair> setItems;
};

class BoundCreateNode : public BoundCreateNodeOrRel {
public:
    BoundCreateNode(std::shared_ptr<NodeExpression> node,
        std::shared_ptr<Expression> primaryKeyExpression, std::vector<expression_pair> setItems)
        : BoundCreateNodeOrRel{std::move(setItems)}, node{std::move(node)},
          primaryKeyExpression{std::move(primaryKeyExpression)} {}

    inline std::shared_ptr<NodeExpression> getNode() const { return node; }
    inline std::shared_ptr<Expression> getPrimaryKeyExpression() const {
        return primaryKeyExpression;
    }

    inline std::unique_ptr<BoundCreateNode> copy() const {
        return make_unique<BoundCreateNode>(node, primaryKeyExpression, setItems);
    }

private:
    std::shared_ptr<NodeExpression> node;
    std::shared_ptr<Expression> primaryKeyExpression;
};

class BoundCreateRel : public BoundCreateNodeOrRel {
public:
    BoundCreateRel(std::shared_ptr<RelExpression> rel, std::vector<expression_pair> setItems)
        : BoundCreateNodeOrRel{std::move(setItems)}, rel{std::move(rel)} {}

    inline std::shared_ptr<RelExpression> getRel() const { return rel; }

    inline std::unique_ptr<BoundCreateRel> copy() const {
        return make_unique<BoundCreateRel>(rel, setItems);
    }

private:
    std::shared_ptr<RelExpression> rel;
};

class BoundCreateClause : public BoundUpdatingClause {
public:
    BoundCreateClause() : BoundUpdatingClause{common::ClauseType::CREATE} {};
    ~BoundCreateClause() override = default;

    inline void addCreateNode(std::unique_ptr<BoundCreateNode> createNode) {
        createNodes.push_back(std::move(createNode));
    }
    inline bool hasCreateNode() const { return !createNodes.empty(); }
    inline const std::vector<std::unique_ptr<BoundCreateNode>>& getCreateNodes() const {
        return createNodes;
    }

    inline void addCreateRel(std::unique_ptr<BoundCreateRel> createRel) {
        createRels.push_back(std::move(createRel));
    }
    inline bool hasCreateRel() const { return !createRels.empty(); }
    inline const std::vector<std::unique_ptr<BoundCreateRel>>& getCreateRels() const {
        return createRels;
    }

    std::vector<expression_pair> getAllSetItems() const;

    std::unique_ptr<BoundUpdatingClause> copy() override;

private:
    std::vector<std::unique_ptr<BoundCreateNode>> createNodes;
    std::vector<std::unique_ptr<BoundCreateRel>> createRels;
};

} // namespace binder
} // namespace kuzu
