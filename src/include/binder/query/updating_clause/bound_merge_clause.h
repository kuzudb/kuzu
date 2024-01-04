#pragma once

#include "binder/query/query_graph.h"
#include "bound_insert_info.h"
#include "bound_set_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundMergeClause : public BoundUpdatingClause {
public:
    BoundMergeClause(QueryGraphCollection queryGraphCollection,
        std::shared_ptr<Expression> predicate, std::vector<BoundInsertInfo> insertInfos)
        : BoundUpdatingClause{common::ClauseType::MERGE}, queryGraphCollection{std::move(
                                                              queryGraphCollection)},
          predicate{std::move(predicate)}, insertInfos{std::move(insertInfos)} {}

    inline const QueryGraphCollection* getQueryGraphCollection() const {
        return &queryGraphCollection;
    }
    inline bool hasPredicate() const { return predicate != nullptr; }
    inline std::shared_ptr<Expression> getPredicate() const { return predicate; }

    inline const std::vector<BoundInsertInfo>& getInsertInfosRef() const { return insertInfos; }
    inline const std::vector<BoundSetPropertyInfo>& getOnMatchSetInfosRef() const {
        return onMatchSetPropertyInfos;
    }
    inline const std::vector<BoundSetPropertyInfo>& getOnCreateSetInfosRef() const {
        return onCreateSetPropertyInfos;
    }

    inline bool hasInsertNodeInfo() const {
        return hasInsertInfo(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::NODE; });
    }
    inline std::vector<const BoundInsertInfo*> getInsertNodeInfos() const {
        return getInsertInfos(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::NODE; });
    }
    inline bool hasInsertRelInfo() const {
        return hasInsertInfo(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::REL; });
    }
    inline std::vector<const BoundInsertInfo*> getInsertRelInfos() const {
        return getInsertInfos(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::REL; });
    }

    inline bool hasOnMatchSetNodeInfo() const {
        return hasOnMatchSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<const BoundSetPropertyInfo*> getOnMatchSetNodeInfos() const {
        return getOnMatchSetInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasOnMatchSetRelInfo() const {
        return hasOnMatchSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<const BoundSetPropertyInfo*> getOnMatchSetRelInfos() const {
        return getOnMatchSetInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    inline bool hasOnCreateSetNodeInfo() const {
        return hasOnCreateSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<const BoundSetPropertyInfo*> getOnCreateSetNodeInfos() const {
        return getOnCreateSetInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasOnCreateSetRelInfo() const {
        return hasOnCreateSetInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<const BoundSetPropertyInfo*> getOnCreateSetRelInfos() const {
        return getOnCreateSetInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    inline void addOnMatchSetPropertyInfo(BoundSetPropertyInfo setPropertyInfo) {
        onMatchSetPropertyInfos.push_back(std::move(setPropertyInfo));
    }
    inline void addOnCreateSetPropertyInfo(BoundSetPropertyInfo setPropertyInfo) {
        onCreateSetPropertyInfos.push_back(std::move(setPropertyInfo));
    }

private:
    bool hasInsertInfo(const std::function<bool(const BoundInsertInfo& info)>& check) const;
    std::vector<const BoundInsertInfo*> getInsertInfos(
        const std::function<bool(const BoundInsertInfo& info)>& check) const;

    bool hasOnMatchSetInfo(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;
    std::vector<const BoundSetPropertyInfo*> getOnMatchSetInfos(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;

    bool hasOnCreateSetInfo(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;
    std::vector<const BoundSetPropertyInfo*> getOnCreateSetInfos(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;

private:
    // Pattern to match.
    QueryGraphCollection queryGraphCollection;
    std::shared_ptr<Expression> predicate;
    // Pattern to create on match failure.
    std::vector<BoundInsertInfo> insertInfos;
    // Update on match
    std::vector<BoundSetPropertyInfo> onMatchSetPropertyInfos;
    // Update on create
    std::vector<BoundSetPropertyInfo> onCreateSetPropertyInfos;
};

} // namespace binder
} // namespace kuzu
