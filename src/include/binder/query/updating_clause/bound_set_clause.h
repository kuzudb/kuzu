#pragma once

#include "binder/expression/rel_expression.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundSetNodeProperty {
public:
    BoundSetNodeProperty(shared_ptr<NodeExpression> node, expression_pair setItem)
        : node{std::move(node)}, setItem{std::move(setItem)} {}

    inline shared_ptr<NodeExpression> getNode() const { return node; }
    inline expression_pair getSetItem() const { return setItem; }

    inline unique_ptr<BoundSetNodeProperty> copy() const {
        return make_unique<BoundSetNodeProperty>(node, setItem);
    }

private:
    shared_ptr<NodeExpression> node;
    expression_pair setItem;
};

class BoundSetRelProperty {
public:
    BoundSetRelProperty(shared_ptr<RelExpression> rel, expression_pair setItem)
        : rel{std::move(rel)}, setItem{std::move(setItem)} {}

    inline shared_ptr<RelExpression> getRel() const { return rel; }
    inline expression_pair getSetItem() const { return setItem; }

    inline unique_ptr<BoundSetRelProperty> copy() const {
        return make_unique<BoundSetRelProperty>(rel, setItem);
    }

private:
    shared_ptr<RelExpression> rel;
    expression_pair setItem;
};

class BoundSetClause : public BoundUpdatingClause {
public:
    BoundSetClause() : BoundUpdatingClause{ClauseType::SET} {};

    inline void addSetNodeProperty(unique_ptr<BoundSetNodeProperty> setNodeProperty) {
        setNodeProperties.push_back(std::move(setNodeProperty));
    }
    inline bool hasSetNodeProperty() const { return !setNodeProperties.empty(); }
    inline const vector<unique_ptr<BoundSetNodeProperty>>& getSetNodeProperties() const {
        return setNodeProperties;
    }

    inline void addSetRelProperty(unique_ptr<BoundSetRelProperty> setRelProperty) {
        setRelProperties.push_back(std::move(setRelProperty));
    }
    inline bool hasSetRelProperty() const { return !setRelProperties.empty(); }
    inline const vector<unique_ptr<BoundSetRelProperty>>& getSetRelProperties() const {
        return setRelProperties;
    }

    expression_vector getPropertiesToRead() const override;

    unique_ptr<BoundUpdatingClause> copy() override;

private:
    vector<unique_ptr<BoundSetNodeProperty>> setNodeProperties;
    vector<unique_ptr<BoundSetRelProperty>> setRelProperties;
};

} // namespace binder
} // namespace kuzu
