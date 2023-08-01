#pragma once

#include "bound_set_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundSetClause : public BoundUpdatingClause {
public:
    BoundSetClause() : BoundUpdatingClause{common::ClauseType::SET} {}
    BoundSetClause(const BoundSetClause& other);

    inline void addInfo(std::unique_ptr<BoundSetPropertyInfo> info) {
        infos.push_back(std::move(info));
    }
    inline const std::vector<std::unique_ptr<BoundSetPropertyInfo>>& getInfosRef() { return infos; }

    inline bool hasNodeInfo() const {
        return hasInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundSetPropertyInfo*> getNodeInfos() const {
        return getInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasRelInfo() const {
        return hasInfo([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundSetPropertyInfo*> getRelInfos() const {
        return getInfos([](const BoundSetPropertyInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    inline std::unique_ptr<BoundUpdatingClause> copy() final {
        return std::make_unique<BoundSetClause>(*this);
    }

private:
    bool hasInfo(const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;
    std::vector<BoundSetPropertyInfo*> getInfos(
        const std::function<bool(const BoundSetPropertyInfo& info)>& check) const;

private:
    std::vector<std::unique_ptr<BoundSetPropertyInfo>> infos;
};

} // namespace binder
} // namespace kuzu
