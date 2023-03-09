#include "processor/operator/intersect/intersect_build.h"

using namespace kuzu::storage;
namespace kuzu {
namespace processor {

void IntersectSharedState::initEmptyHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
    std::unique_ptr<FactorizedTableSchema> tableSchema) {
    assert(hashTable == nullptr && numKeyColumns == 1);
    hashTable = make_unique<IntersectHashTable>(memoryManager, std::move(tableSchema));
}

void IntersectBuild::initLocalHashTable(
    MemoryManager& memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema) {
    hashTable = make_unique<IntersectHashTable>(
        memoryManager, std::make_unique<FactorizedTableSchema>(*tableSchema));
}

} // namespace processor
} // namespace kuzu
