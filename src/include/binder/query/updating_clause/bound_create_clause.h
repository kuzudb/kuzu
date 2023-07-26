#pragma once

#include "bound_create_info.h"
#include "bound_updating_clause.h"

namespace kuzu {
namespace binder {

class BoundCreateClause : public BoundUpdatingClause {
public:
    explicit BoundCreateClause(std::vector<std::unique_ptr<BoundCreateInfo>> infos)
        : BoundUpdatingClause{common::ClauseType::CREATE}, infos{std::move(infos)} {}
    BoundCreateClause(const BoundCreateClause& other);

    inline const std::vector<std::unique_ptr<BoundCreateInfo>>& getInfosRef() { return infos; }

    inline bool hasNodeInfo() const {
        return hasInfo([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline std::vector<BoundCreateInfo*> getNodeInfos() const {
        return getInfos([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::NODE;
        });
    }
    inline bool hasRelInfo() const {
        return hasInfo([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }
    inline std::vector<BoundCreateInfo*> getRelInfos() const {
        return getInfos([](const BoundCreateInfo& info) {
            return info.updateTableType == UpdateTableType::REL;
        });
    }

    std::vector<expression_pair> getAllSetItems() const;

    inline std::unique_ptr<BoundUpdatingClause> copy() final {
        return std::make_unique<BoundCreateClause>(*this);
    }

private:
    bool hasInfo(const std::function<bool(const BoundCreateInfo& info)>& check) const;
    std::vector<BoundCreateInfo*> getInfos(
        const std::function<bool(const BoundCreateInfo& info)>& check) const;

private:
    std::vector<std::unique_ptr<BoundCreateInfo>> infos;
};

} // namespace binder
} // namespace kuzu
