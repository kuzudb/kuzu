#include "processor/operator/intersect/intersect_build.h"

namespace kuzu {
namespace processor {

void IntersectSharedState::initEmptyHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
    unique_ptr<FactorizedTableSchema> tableSchema) {
    assert(hashTable == nullptr && numKeyColumns == 1);
    hashTable = make_unique<IntersectHashTable>(memoryManager, std::move(tableSchema));
}

void IntersectBuild::initLocalHashTable(
    MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) {
    hashTable = make_unique<IntersectHashTable>(
        memoryManager, make_unique<FactorizedTableSchema>(*tableSchema));
}

} // namespace processor
} // namespace kuzu
