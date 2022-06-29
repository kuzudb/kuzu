#pragma once

#include "bound_updating_clause.h"
#include "update_info.h"

namespace graphflow {
namespace binder {

class BoundCreateClause : public BoundUpdatingClause {
public:
    BoundCreateClause() : BoundUpdatingClause{ClauseType::CREATE} {};
    ~BoundCreateClause() override = default;

    inline void addNodeUpdateInfo(unique_ptr<NodeUpdateInfo> updateInfo) {
        nodeUpdateInfos.push_back(move(updateInfo));
    }
    inline uint32_t getNumNodeUpdateInfo() const { return nodeUpdateInfos.size(); }
    inline NodeUpdateInfo* getNodeUpdateInfo(uint32_t idx) const {
        return nodeUpdateInfos[idx].get();
    }

    inline expression_vector getPropertiesToRead() const override {
        expression_vector result;
        for (auto& nodeUpdateInfo : nodeUpdateInfos) {
            for (auto& property : nodeUpdateInfo->getPropertiesToRead()) {
                result.push_back(property);
            }
        }
        return result;
    }

    inline unique_ptr<BoundUpdatingClause> copy() override {
        auto result = make_unique<BoundCreateClause>();
        for (auto& nodeUpdateInfo : nodeUpdateInfos) {
            result->addNodeUpdateInfo(make_unique<NodeUpdateInfo>(*nodeUpdateInfo));
        }
        return result;
    }

private:
    vector<unique_ptr<NodeUpdateInfo>> nodeUpdateInfos;
};

} // namespace binder
} // namespace graphflow
