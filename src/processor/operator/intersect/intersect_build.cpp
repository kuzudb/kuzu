#include "processor/operator/intersect/intersect_build.h"

namespace kuzu {
namespace processor {

void IntersectSharedState::initEmptyHashTableIfNecessary(MemoryManager& memoryManager,
    uint64_t numKeyColumns, unique_ptr<FactorizedTableSchema> tableSchema) {
    unique_lock lck(hashJoinSharedStateMutex);
    assert(numKeyColumns == 1);
    if (hashTable == nullptr) {
        hashTable = make_unique<IntersectHashTable>(memoryManager, move(tableSchema));
    }
}

void IntersectBuild::initHashTable(
    MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) {
    hashTable = make_unique<IntersectHashTable>(
        memoryManager, make_unique<FactorizedTableSchema>(*tableSchema));
    sharedState->initEmptyHashTableIfNecessary(
        memoryManager, buildDataInfo.keysDataPos.size(), std::move(tableSchema));
}

} // namespace processor
} // namespace kuzu
