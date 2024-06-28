#pragma once

#include "binder/query/query_graph.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

struct BoundJoinHintNode {
    std::shared_ptr<Expression> nodeOrRel;
    std::vector<std::shared_ptr<BoundJoinHintNode>> children;

    BoundJoinHintNode() = default;
    explicit BoundJoinHintNode(std::shared_ptr<Expression> nodeOrRel)
        : nodeOrRel{std::move(nodeOrRel)} {}

    void addChild(std::shared_ptr<BoundJoinHintNode> child) {
        children.push_back(std::move(child));
    }

    bool isLeaf() const { return children.empty(); }
    bool isBinary() const { return children.size() == 2; }
    bool isMultiWay() const { return children.size() > 2; }
};

class BoundMatchClause : public BoundReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::MATCH;

public:
    BoundMatchClause(QueryGraphCollection collection, common::MatchClauseType matchClauseType)
        : BoundReadingClause{clauseType_}, collection{std::move(collection)},
          matchClauseType{matchClauseType} {}

    QueryGraphCollection* getQueryGraphCollectionUnsafe() { return &collection; }
    const QueryGraphCollection* getQueryGraphCollection() const { return &collection; }

    common::MatchClauseType getMatchClauseType() const { return matchClauseType; }

    void setHint(std::shared_ptr<BoundJoinHintNode> root) { hintRoot = std::move(root); }
    bool hasHint() const { return hintRoot != nullptr; }
    std::shared_ptr<BoundJoinHintNode> getHint() const { return hintRoot; }

private:
    QueryGraphCollection collection;
    common::MatchClauseType matchClauseType;
    std::shared_ptr<BoundJoinHintNode> hintRoot;
};

} // namespace binder
} // namespace kuzu
