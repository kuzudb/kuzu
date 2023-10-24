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
    std::vector<std::unique_ptr<LogicalType>> keyTypes;
    for (auto i = 0u; i < info->keysPos.size(); ++i) {
        auto vector = resultSet->getValueVector(info->keysPos[i]).get();
        keyTypes.push_back(vector->dataType.copy());
        if (info->fStateTypes[i] == common::FStateType::UNFLAT) {
            setKeyState(vector->state.get());
        }
        keyVectors.push_back(vector);
    }
    if (keyState == nullptr) {
        setKeyState(keyVectors[0]->state.get());
    }
    for (auto& pos : info->payloadsPos) {
        payloadVectors.push_back(resultSet->getValueVector(pos).get());
    }
    hashTable = std::make_unique<JoinHashTable>(
        *context->memoryManager, std::move(keyTypes), info->tableSchema->copy());
}

void HashJoinBuild::setKeyState(common::DataChunkState* state) {
    if (keyState == nullptr) {
        keyState = state;
    } else {
        assert(keyState == state); // two pointers should be pointing to the same state
    }
}

void HashJoinBuild::finalize(ExecutionContext* /*context*/) {
    auto numTuples = sharedState->getHashTable()->getNumTuples();
    sharedState->getHashTable()->allocateHashSlots(numTuples);
    sharedState->getHashTable()->buildHashSlots();
}

void HashJoinBuild::executeInternal(ExecutionContext* context) {
    // Append thread-local tuples
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < resultSet->multiplicity; ++i) {
            appendVectors();
        }
    }
    // Merge with global hash table once local tuples are all appended.
    sharedState->mergeLocalHashTable(*hashTable);
}

} // namespace processor
} // namespace kuzu
