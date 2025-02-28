#include "processor/operator/gds_call_shared_state.h"

#include <mutex>

#include "graph/on_disk_graph.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

offset_t NodeOffsetMaskMap::getNumMaskedNode() const {
    offset_t numNodes = 0;
    for (auto& [tableID, mask] : maskMap) {
        numNodes += mask->getNumMaskedNodes();
    }
    return numNodes;
}

void GDSCallSharedState::setGraphNodeMask(std::unique_ptr<NodeOffsetMaskMap> maskMap) {
    auto onDiskGraph = ku_dynamic_cast<graph::OnDiskGraph*>(graph.get());
    onDiskGraph->setNodeOffsetMask(maskMap.get());
    graphNodeMask = std::move(maskMap);
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
