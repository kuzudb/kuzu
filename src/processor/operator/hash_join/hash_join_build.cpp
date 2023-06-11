#include "processor/operator/hash_join/hash_join_build.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void HashJoinSharedState::mergeLocalHashTable(JoinHashTable& localHashTable) {
    std::unique_lock lck(mtx);
    hashTable->merge(localHashTable);
}

void HashJoinBuild::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& pos : info->keysPos) {
        vectorsToAppend.push_back(resultSet->getValueVector(pos).get());
    }
    for (auto& pos : info->payloadsPos) {
        vectorsToAppend.push_back(resultSet->getValueVector(pos).get());
    }
    initLocalHashTable(*context->memoryManager);
}

void HashJoinBuild::finalize(ExecutionContext* context) {
    auto numTuples = sharedState->getHashTable()->getNumTuples();
    sharedState->getHashTable()->allocateHashSlots(numTuples);
    sharedState->getHashTable()->buildHashSlots();
}

void HashJoinBuild::executeInternal(ExecutionContext* context) {
    // Append thread-local tuples
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < resultSet->multiplicity; ++i) {
            hashTable->append(vectorsToAppend);
        }
    }
    // Merge with global hash table once local tuples are all appended.
    sharedState->mergeLocalHashTable(*hashTable);
}

} // namespace processor
} // namespace kuzu
