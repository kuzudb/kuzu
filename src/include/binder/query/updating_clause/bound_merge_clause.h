#pragma once

#include "binder/query/reading_clause/query_graph.h"
#include "bound_create_info.h"
#include "bound_set_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundMergeClause : public BoundUpdatingClause {
public:
    BoundMergeClause(std::unique_ptr<QueryGraphCollection> queryGraphCollection,
        std::vector<std::unique_ptr<BoundCreateInfo>> createInfos)
        : BoundUpdatingClause{common::ClauseType::MERGE},
          queryGraphCollection{std::move(queryGraphCollection)}, createInfos{
                                                                     std::move(createInfos)} {}
    BoundMergeClause(const BoundMergeClause& other);

    inline void addOnMatchSetPropertyInfo(std::unique_ptr<BoundSetPropertyInfo> setPropertyInfo) {
        onMatchSetPropertyInfos.push_back(std::move(setPropertyInfo));
    }
    inline void addOnCreateSetPropertyInfo(std::unique_ptr<BoundSetPropertyInfo> setPropertyInfo) {
        onCreateSetPropertyInfos.push_back(std::move(setPropertyInfo));
    }

    std::unique_ptr<BoundUpdatingClause> copy() final {
        return std::make_unique<BoundMergeClause>(*this);
    }

private:
    // Pattern to match.
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    // Pattern to create on match failure.
    std::vector<std::unique_ptr<BoundCreateInfo>> createInfos;
    // Update on match
    std::vector<std::unique_ptr<BoundSetPropertyInfo>> onMatchSetPropertyInfos;
    // Update on create
    std::vector<std::unique_ptr<BoundSetPropertyInfo>> onCreateSetPropertyInfos;
};

} // namespace binder
} // namespace kuzu
