#pragma once

#include "bound_delete_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundDeleteClause : public BoundUpdatingClause {
public:
    BoundDeleteClause() : BoundUpdatingClause{common::ClauseType::DELETE_} {};
    BoundDeleteClause(const BoundDeleteClause& other);

    inline void addInfo(std::unique_ptr<BoundDeleteInfo> info) { infos.push_back(std::move(info)); }

    inline bool hasNodeInfo() const {
        return hasInfo([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundDeleteInfo*> getNodeInfos() const {
        return getInfos([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasRelInfo() const {
        return hasInfo([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundDeleteInfo*> getRelInfos() const {
        return getInfos([](const BoundDeleteInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    inline std::unique_ptr<BoundUpdatingClause> copy() final {
        return std::make_unique<BoundDeleteClause>(*this);
    }

private:
    bool hasInfo(const std::function<bool(const BoundDeleteInfo& info)>& check) const;
    std::vector<BoundDeleteInfo*> getInfos(
        const std::function<bool(const BoundDeleteInfo& info)>& check) const;

private:
    std::vector<std::unique_ptr<BoundDeleteInfo>> infos;
};

} // namespace binder
} // namespace kuzu
