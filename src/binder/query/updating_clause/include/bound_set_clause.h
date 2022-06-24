#pragma once

#include "bound_updating_clause.h"
#include "update_info.h"

namespace graphflow {
namespace binder {

class BoundSetClause : public BoundUpdatingClause {

public:
    BoundSetClause() : BoundUpdatingClause{ClauseType::SET} {};
    ~BoundSetClause() override = default;

    inline void addUpdateInfo(unique_ptr<PropertyUpdateInfo> updateInfo) {
        propertyUpdateInfos.push_back(move(updateInfo));
    }
    inline uint32_t getNumPropertyUpdateInfos() const { return propertyUpdateInfos.size(); }
    inline PropertyUpdateInfo* getPropertyUpdateInfo(uint32_t idx) const {
        return propertyUpdateInfos[idx].get();
    }

    inline expression_vector getPropertiesToRead() const override {
        expression_vector result;
        for (auto& propertyUpdateInfo : propertyUpdateInfos) {
            for (auto& property : propertyUpdateInfo->getPropertiesToRead()) {
                result.push_back(property);
            }
        }
        return result;
    }

    inline unique_ptr<BoundUpdatingClause> copy() override {
        auto result = make_unique<BoundSetClause>();
        for (auto& updateInfo : propertyUpdateInfos) {
            result->addUpdateInfo(make_unique<PropertyUpdateInfo>(*updateInfo));
        }
        return result;
    }

private:
    vector<unique_ptr<PropertyUpdateInfo>> propertyUpdateInfos;
};

} // namespace binder
} // namespace graphflow
