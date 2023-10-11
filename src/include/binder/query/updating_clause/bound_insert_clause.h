#pragma once

#include "bound_insert_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundInsertClause : public BoundUpdatingClause {
public:
    explicit BoundInsertClause(std::vector<std::unique_ptr<BoundInsertInfo>> infos)
        : BoundUpdatingClause{common::ClauseType::INSERT}, infos{std::move(infos)} {}
    BoundInsertClause(const BoundInsertClause& other);

    inline const std::vector<std::unique_ptr<BoundInsertInfo>>& getInfosRef() { return infos; }

    inline bool hasNodeInfo() const {
        return hasInfo([](const BoundInsertInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundInsertInfo*> getNodeInfos() const {
        return getInfos([](const BoundInsertInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasRelInfo() const {
        return hasInfo([](const BoundInsertInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundInsertInfo*> getRelInfos() const {
        return getInfos([](const BoundInsertInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    inline std::unique_ptr<BoundUpdatingClause> copy() final {
        return std::make_unique<BoundInsertClause>(*this);
    }

private:
    bool hasInfo(const std::function<bool(const BoundInsertInfo& info)>& check) const;
    std::vector<BoundInsertInfo*> getInfos(
        const std::function<bool(const BoundInsertInfo& info)>& check) const;

private:
    std::vector<std::unique_ptr<BoundInsertInfo>> infos;
};

} // namespace binder
} // namespace kuzu
