#pragma once

#include "bound_set_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundSetClause : public BoundUpdatingClause {
public:
    BoundSetClause() : BoundUpdatingClause{common::ClauseType::SET} {}

    inline void addInfo(BoundSetPropertyInfo info) { infos.push_back(std::move(info)); }
    inline const std::vector<BoundSetPropertyInfo>& getInfosRef() { return infos; }

    inline bool hasNodeInfo() const {
        return hasInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<const BoundSetPropertyInfo*> getNodeInfos() const {
        return getInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasRelInfo() const {
        return hasInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<const BoundSetPropertyInfo*> getRelInfos() const {
        return getInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

private:
    bool hasInfo(const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;
    std::vector<const BoundSetPropertyInfo*> getInfos(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;

private:
    std::vector<BoundSetPropertyInfo> infos;
};

} // namespace binder
} // namespace kuzu
