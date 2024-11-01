#include "processor/operator/gds_call_shared_state.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

offset_t NodeOffsetMaskMap::getNumMaskedNode() const {
    KU_ASSERT(enabled_);
    offset_t numNodes = 0;
    for (auto& [tableID, mask] : maskMap) {
        numNodes += mask->getNumMaskedNodes();
    }
    return numNodes;
}

FactorizedTable* GDSCallSharedState::claimLocalTable(storage::MemoryManager* mm) {
    std::unique_lock<std::mutex> lck{mtx};
    if (availableLocalTables.empty()) {
        auto table =
            std::make_shared<processor::FactorizedTable>(mm, fTable->getTableSchema()->copy());
        localTables.push_back(table);
        availableLocalTables.push(table.get());
    }
    auto result = availableLocalTables.top();
    availableLocalTables.pop();
    return result;
}

void GDSCallSharedState::returnLocalTable(FactorizedTable* table) {
    std::unique_lock<std::mutex> lck{mtx};
    availableLocalTables.push(table);
}

void GDSCallSharedState::mergeLocalTables() {
    std::unique_lock<std::mutex> lck{mtx};
    for (auto& localTable : localTables) {
        fTable->merge(*localTable);
    }
}

} // namespace processor
} // namespace kuzu
