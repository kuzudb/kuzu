#pragma once

#include "binder/expression/rel_expression.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundSetNodeProperty {
public:
    BoundSetNodeProperty(std::shared_ptr<NodeExpression> node, expression_pair setItem)
        : node{std::move(node)}, setItem{std::move(setItem)} {}

    inline std::shared_ptr<NodeExpression> getNode() const { return node; }
    inline expression_pair getSetItem() const { return setItem; }

    inline std::unique_ptr<BoundSetNodeProperty> copy() const {
        return make_unique<BoundSetNodeProperty>(node, setItem);
    }

private:
    std::shared_ptr<NodeExpression> node;
    expression_pair setItem;
};

class BoundSetRelProperty {
public:
    BoundSetRelProperty(std::shared_ptr<RelExpression> rel, expression_pair setItem)
        : rel{std::move(rel)}, setItem{std::move(setItem)} {}

    inline std::shared_ptr<RelExpression> getRel() const { return rel; }
    inline expression_pair getSetItem() const { return setItem; }

    inline std::unique_ptr<BoundSetRelProperty> copy() const {
        return make_unique<BoundSetRelProperty>(rel, setItem);
    }

private:
    std::shared_ptr<RelExpression> rel;
    expression_pair setItem;
};

class BoundSetClause : public BoundUpdatingClause {
public:
    BoundSetClause() : BoundUpdatingClause{common::ClauseType::SET} {}

    inline void addSetNodeProperty(std::unique_ptr<BoundSetNodeProperty> setNodeProperty) {
        setNodeProperties.push_back(std::move(setNodeProperty));
    }
    inline bool hasSetNodeProperty() const { return !setNodeProperties.empty(); }
    inline const std::vector<std::unique_ptr<BoundSetNodeProperty>>& getSetNodeProperties() const {
        return setNodeProperties;
    }

    inline void addSetRelProperty(std::unique_ptr<BoundSetRelProperty> setRelProperty) {
        setRelProperties.push_back(std::move(setRelProperty));
    }
    inline bool hasSetRelProperty() const { return !setRelProperties.empty(); }
    inline const std::vector<std::unique_ptr<BoundSetRelProperty>>& getSetRelProperties() const {
        return setRelProperties;
    }

    std::unique_ptr<BoundUpdatingClause> copy() override;

private:
    std::vector<std::unique_ptr<BoundSetNodeProperty>> setNodeProperties;
    std::vector<std::unique_ptr<BoundSetRelProperty>> setRelProperties;
};

} // namespace binder
} // namespace kuzu
