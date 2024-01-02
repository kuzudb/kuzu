#pragma once

#include "bound_delete_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundDeleteClause : public BoundUpdatingClause {
public:
    BoundDeleteClause() : BoundUpdatingClause{common::ClauseType::DELETE_} {};

    inline void addInfo(BoundDeleteInfo info) { infos.push_back(std::move(info)); }

    inline bool hasNodeInfo() const {
        return hasInfo([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<const BoundDeleteInfo*> getNodeInfos() const {
        return getInfos([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasRelInfo() const {
        return hasInfo([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<const BoundDeleteInfo*> getRelInfos() const {
        return getInfos([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

private:
    bool hasInfo(const std::function<bool(const BoundDeleteInfo& info)>& check) const;
    std::vector<const BoundDeleteInfo*> getInfos(
        const std::function<bool(const BoundDeleteInfo& info)>& check) const;

private:
    std::vector<BoundDeleteInfo> infos;
};

} // namespace binder
} // namespace kuzu
