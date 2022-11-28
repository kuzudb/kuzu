#include "processor/operator/hash_join/hash_join_build.h"

#include "common/utils.h"

namespace kuzu {
namespace processor {

void HashJoinSharedState::initEmptyHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
    unique_ptr<FactorizedTableSchema> tableSchema) {
    assert(hashTable == nullptr);
    hashTable = make_unique<JoinHashTable>(memoryManager, numKeyColumns, std::move(tableSchema));
}

void HashJoinSharedState::mergeLocalHashTable(JoinHashTable& localHashTable) {
    unique_lock lck(mtx);
    hashTable->merge(localHashTable);
}

shared_ptr<ResultSet> HashJoinBuild::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    for (auto& [pos, dataType] : buildDataInfo.keysPosAndType) {
        auto keyVector = resultSet->getValueVector(pos);
        vectorsToAppend.push_back(keyVector);
    }
    for (auto& [pos, dataType] : buildDataInfo.payloadsPosAndType) {
        auto dataChunk = resultSet->dataChunks[pos.dataChunkPos];
        auto vector = dataChunk->valueVectors[pos.valueVectorPos];
        vectorsToAppend.push_back(vector);
    }
    auto tableSchema = populateTableSchema();
    initLocalHashTable(*context->memoryManager, std::move(tableSchema));
    return resultSet;
}

unique_ptr<FactorizedTableSchema> HashJoinBuild::populateTableSchema() {
    unique_ptr<FactorizedTableSchema> tableSchema = make_unique<FactorizedTableSchema>();
    for (auto& [pos, dataType] : buildDataInfo.keysPosAndType) {
        tableSchema->appendColumn(make_unique<ColumnSchema>(
            false /* is flat */, pos.dataChunkPos, Types::getDataTypeSize(dataType)));
    }
    for (auto i = 0u; i < buildDataInfo.payloadsPosAndType.size(); ++i) {
        auto [pos, dataType] = buildDataInfo.payloadsPosAndType[i];
        if (buildDataInfo.isPayloadsInKeyChunk[i]) {
            tableSchema->appendColumn(make_unique<ColumnSchema>(
                false /* is flat */, pos.dataChunkPos, Types::getDataTypeSize(dataType)));
        } else {
            auto isVectorFlat = buildDataInfo.isPayloadsFlat[i];
            tableSchema->appendColumn(make_unique<ColumnSchema>(!isVectorFlat, pos.dataChunkPos,
                isVectorFlat ? Types::getDataTypeSize(dataType) :
                               (uint32_t)sizeof(overflow_value_t)));
        }
    }
    // The prev pointer column.
    tableSchema->appendColumn(make_unique<ColumnSchema>(false /* is flat */,
        UINT32_MAX /* For now, we just put UINT32_MAX for prev pointer */,
        Types::getDataTypeSize(INT64)));
    return tableSchema;
}

void HashJoinBuild::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->initEmptyHashTable(
        *context->memoryManager, buildDataInfo.getNumKeys(), populateTableSchema());
}

void HashJoinBuild::initLocalHashTable(
    MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) {
    hashTable = make_unique<JoinHashTable>(memoryManager, buildDataInfo.getNumKeys(),
        make_unique<FactorizedTableSchema>(*tableSchema));
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
