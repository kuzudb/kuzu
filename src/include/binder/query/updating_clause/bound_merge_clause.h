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
        std::shared_ptr<Expression> predicate,
        std::vector<std::unique_ptr<BoundCreateInfo>> createInfos)
        : BoundUpdatingClause{common::ClauseType::MERGE}, queryGraphCollection{std::move(
                                                              queryGraphCollection)},
          predicate{std::move(predicate)}, createInfos{std::move(createInfos)} {}
    BoundMergeClause(const BoundMergeClause& other);

    inline QueryGraphCollection* getQueryGraphCollection() const {
        return queryGraphCollection.get();
    }
    inline std::shared_ptr<Expression> getPredicate() const { return predicate; }

    inline const std::vector<std::unique_ptr<BoundCreateInfo>>& getCreateInfosRef() const {
        return createInfos;
    }

    inline bool hasNodeCreateInfo() const {
        return hasCreateInfo([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundCreateInfo*> getNodeCreateInfos() const {
        return getCreateInfos([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasRelCreateInfo() const {
        return hasCreateInfo([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundCreateInfo*> getRelCreateInfos() const {
        return getCreateInfos([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

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
    bool hasCreateInfo(const std::function<bool(const BoundCreateInfo& info)>& check) const;
    std::vector<BoundCreateInfo*> getCreateInfos(
        const std::function<bool(const BoundCreateInfo& info)>& check) const;

private:
    // Pattern to match.
    std::unique_ptr<QueryGraphCollection> queryGraphCollection;
    std::shared_ptr<Expression> predicate;
    // Pattern to create on match failure.
    std::vector<std::unique_ptr<BoundCreateInfo>> createInfos;
    // Update on match
    std::vector<std::unique_ptr<BoundSetPropertyInfo>> onMatchSetPropertyInfos;
    // Update on create
    std::vector<std::unique_ptr<BoundSetPropertyInfo>> onCreateSetPropertyInfos;
};

} // namespace binder
} // namespace kuzu
