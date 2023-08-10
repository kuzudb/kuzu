#pragma once

#include "binder/query/query_graph.h"
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
    inline bool hasPredicate() const { return predicate != nullptr; }
    inline std::shared_ptr<Expression> getPredicate() const { return predicate; }

    inline const std::vector<std::unique_ptr<BoundCreateInfo>>& getCreateInfosRef() const {
        return createInfos;
    }
    inline const std::vector<std::unique_ptr<BoundSetPropertyInfo>>& getOnMatchSetInfosRef() const {
        return onMatchSetPropertyInfos;
    }
    inline const std::vector<std::unique_ptr<BoundSetPropertyInfo>>&
    getOnCreateSetInfosRef() const {
        return onCreateSetPropertyInfos;
    }

    inline bool hasCreateNodeInfo() const {
        return hasCreateInfo([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundCreateInfo*> getCreateNodeInfos() const {
        return getCreateInfos([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasCreateRelInfo() const {
        return hasCreateInfo([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundCreateInfo*> getCreateRelInfos() const {
        return getCreateInfos([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    inline bool hasOnMatchSetNodeInfo() const {
        return hasOnMatchSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundSetPropertyInfo*> getOnMatchSetNodeInfos() const {
        return getOnMatchSetInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasOnMatchSetRelInfo() const {
        return hasOnMatchSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundSetPropertyInfo*> getOnMatchSetRelInfos() const {
        return getOnMatchSetInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    inline bool hasOnCreateSetNodeInfo() const {
        return hasOnCreateSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundSetPropertyInfo*> getOnCreateSetNodeInfos() const {
        return getOnCreateSetInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasOnCreateSetRelInfo() const {
        return hasOnCreateSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundSetPropertyInfo*> getOnCreateSetRelInfos() const {
        return getOnCreateSetInfos([](const BoundSetPropertyInfo& info) {
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

    bool hasOnMatchSetInfo(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;
    std::vector<BoundSetPropertyInfo*> getOnMatchSetInfos(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;

    bool hasOnCreateSetInfo(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;
    std::vector<BoundSetPropertyInfo*> getOnCreateSetInfos(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;

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
