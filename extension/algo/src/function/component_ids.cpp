#include "function/component_ids.h"

using namespace kuzu::common;

namespace kuzu {
namespace algo_extension {

OffsetManager::OffsetManager(const table_id_map_t<offset_t>& maxOffsetMap) {
    std::vector<table_id_t> tableIDVector;
    for (auto [tableID, maxOffset] : maxOffsetMap) {
        tableIDVector.push_back(tableID);
    }
    std::sort(tableIDVector.begin(), tableIDVector.end());
    auto offset = 0u;
    for (auto tableID : tableIDVector) {
        tableIDToStartOffset.insert({tableID, offset});
        offset += maxOffsetMap.at(tableID);
    }
}

ComponentIDs ComponentIDs::getSequenceComponentIDs(const table_id_map_t<offset_t>& maxOffsetMap,
    const OffsetManager& offsetManager, storage::MemoryManager* mm) {
    auto result = ComponentIDs();
    for (auto [tableID, maxOffset] : maxOffsetMap) {
        result.denseObjects.allocate(tableID, maxOffset, mm);
        result.pinTableID(tableID);
        auto startOffset = offsetManager.getStartOffset(tableID);
        for (auto i = 0u; i < maxOffset; i++) {
            result.setComponentID(i, startOffset + i);
        }
    }
    return result;
}

ComponentIDs ComponentIDs::getUnvisitedComponentIDs(const table_id_map_t<offset_t>& maxOffsetMap,
    storage::MemoryManager* mm) {
    auto result = ComponentIDs();
    for (auto [tableID, maxOffset] : maxOffsetMap) {
        result.denseObjects.allocate(tableID, maxOffset, mm);
        result.pinTableID(tableID);
        for (auto i = 0u; i < maxOffset; i++) {
            result.setComponentID(i, INVALID_COMPONENT_ID);
        }
    }
    return result;
}

bool ComponentIDsPair::update(offset_t boundOffset, offset_t nbrOffset) {
    auto boundValue = curData[boundOffset].load(std::memory_order_relaxed);
    auto tmp = nextData[nbrOffset].load(std::memory_order_relaxed);
    while (tmp > boundValue) {
        if (nextData[nbrOffset].compare_exchange_strong(tmp, boundValue)) {
            return true;
        }
    }
    return false;
}

void ComponentIDsOutputVertexCompute::vertexCompute(offset_t startOffset, offset_t endOffset,
    table_id_t tableID) {
    for (auto i = startOffset; i < endOffset; ++i) {
        if (skip(i)) {
            continue;
        }
        auto nodeID = nodeID_t{i, tableID};
        nodeIDVector->setValue<nodeID_t>(0, nodeID);
        componentIDVector->setValue<uint64_t>(0, componentIDs.getComponentID(i));
        localFT->append(vectors);
    }
}

} // namespace algo_extension
} // namespace kuzu
