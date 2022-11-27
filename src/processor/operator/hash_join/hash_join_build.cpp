#include "processor/operator/hash_join/hash_join_build.h"

#include "common/utils.h"

namespace kuzu {
namespace processor {

void HashJoinSharedState::initEmptyHashTableIfNecessary(MemoryManager& memoryManager,
    uint64_t numKeyColumns, unique_ptr<FactorizedTableSchema> tableSchema) {
    unique_lock lck(hashJoinSharedStateMutex);
    if (hashTable == nullptr) {
        hashTable = make_unique<JoinHashTable>(memoryManager, numKeyColumns, move(tableSchema));
    }
}

void HashJoinSharedState::mergeLocalHashTable(JoinHashTable& localHashTable) {
    unique_lock lck(hashJoinSharedStateMutex);
    hashTable->merge(localHashTable);
}

shared_ptr<ResultSet> HashJoinBuild::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    unique_ptr<FactorizedTableSchema> tableSchema = make_unique<FactorizedTableSchema>();
    for (auto& keyDataPos : buildDataInfo.keysDataPos) {
        auto keyVector = resultSet->getValueVector(keyDataPos);
        tableSchema->appendColumn(make_unique<ColumnSchema>(false /* is flat */,
            keyDataPos.dataChunkPos, Types::getDataTypeSize(keyVector->dataType)));
        vectorsToAppend.push_back(keyVector);
    }
    for (auto i = 0u; i < buildDataInfo.payloadsDataPos.size(); ++i) {
        auto dataChunkPos = buildDataInfo.payloadsDataPos[i].dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = buildDataInfo.payloadsDataPos[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        if (buildDataInfo.isPayloadsInKeyChunk[i]) {
            tableSchema->appendColumn(make_unique<ColumnSchema>(
                false /* is flat */, dataChunkPos, Types::getDataTypeSize(vector->dataType)));
        } else {
            auto isVectorFlat = buildDataInfo.isPayloadsFlat[i];
            tableSchema->appendColumn(make_unique<ColumnSchema>(!isVectorFlat, dataChunkPos,
                isVectorFlat ? Types::getDataTypeSize(vector->dataType) :
                               (uint32_t)sizeof(overflow_value_t)));
        }
        vectorsToAppend.push_back(vector);
    }
    // The prev pointer column.
    tableSchema->appendColumn(make_unique<ColumnSchema>(false /* is flat */,
        UINT32_MAX /* For now, we just put UINT32_MAX for prev pointer */,
        Types::getDataTypeSize(INT64)));
    initHashTable(*context->memoryManager, move(tableSchema));
    return resultSet;
}

void HashJoinBuild::initHashTable(
    MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) {
    hashTable = make_unique<JoinHashTable>(memoryManager, buildDataInfo.keysDataPos.size(),
        make_unique<FactorizedTableSchema>(*tableSchema));
    sharedState->initEmptyHashTableIfNecessary(
        memoryManager, buildDataInfo.keysDataPos.size(), std::move(tableSchema));
}

void HashJoinBuild::finalize(ExecutionContext* context) {
    auto numTuples = sharedState->getHashTable()->getNumTuples();
    sharedState->getHashTable()->allocateHashSlots(numTuples);
    sharedState->getHashTable()->buildHashSlots();
}

void HashJoinBuild::executeInternal(ExecutionContext* context) {
    // Append thread-local tuples
    while (children[0]->getNextTuple()) {
        for (auto i = 0u; i < resultSet->multiplicity; ++i) {
            appendVectors();
        }
    }
    // Merge with global hash table once local tuples are all appended.
    sharedState->mergeLocalHashTable(*hashTable);
}

} // namespace processor
} // namespace kuzu
