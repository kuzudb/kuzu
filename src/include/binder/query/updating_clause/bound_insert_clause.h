#pragma once

#include "bound_insert_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundInsertClause : public BoundUpdatingClause {
public:
    explicit BoundInsertClause(std::vector<BoundInsertInfo> infos)
        : BoundUpdatingClause{common::ClauseType::INSERT}, infos{std::move(infos)} {}

    inline const std::vector<BoundInsertInfo>& getInfosRef() { return infos; }

    inline bool hasNodeInfo() const {
        return hasInfo(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::NODE; });
    }
    inline std::vector<const BoundInsertInfo*> getNodeInfos() const {
        return getInfos(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::NODE; });
    }
    inline bool hasRelInfo() const {
        return hasInfo(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::REL; });
    }
    inline std::vector<const BoundInsertInfo*> getRelInfos() const {
        return getInfos(
            [](const BoundInsertInfo& info) { return info.tableType == common::TableType::REL; });
    }

private:
    bool hasInfo(const std::function<bool(const BoundInsertInfo& info)>& check) const;
    std::vector<const BoundInsertInfo*> getInfos(
        const std::function<bool(const BoundInsertInfo& info)>& check) const;

private:
    std::vector<BoundInsertInfo> infos;
};

} // namespace binder
} // namespace kuzu
