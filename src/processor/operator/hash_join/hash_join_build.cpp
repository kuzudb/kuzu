#include "processor/operator/hash_join/hash_join_build.h"

#include "common/utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void HashJoinSharedState::initEmptyHashTable(MemoryManager& memoryManager, uint64_t numKeyColumns,
    std::unique_ptr<FactorizedTableSchema> tableSchema) {
    assert(hashTable == nullptr);
    hashTable =
        std::make_unique<JoinHashTable>(memoryManager, numKeyColumns, std::move(tableSchema));
}

void HashJoinSharedState::mergeLocalHashTable(JoinHashTable& localHashTable) {
    std::unique_lock lck(mtx);
    hashTable->merge(localHashTable);
}

void HashJoinBuild::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    for (auto& [pos, dataType] : buildDataInfo.keysPosAndType) {
        vectorsToAppend.push_back(resultSet->getValueVector(pos).get());
    }
    for (auto& [pos, dataType] : buildDataInfo.payloadsPosAndType) {
        vectorsToAppend.push_back(resultSet->getValueVector(pos).get());
    }
    auto tableSchema = populateTableSchema();
    initLocalHashTable(*context->memoryManager, std::move(tableSchema));
}

std::unique_ptr<FactorizedTableSchema> HashJoinBuild::populateTableSchema() {
    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();
    for (auto& [pos, dataType] : buildDataInfo.keysPosAndType) {
        tableSchema->appendColumn(std::make_unique<ColumnSchema>(
            false /* is flat */, pos.dataChunkPos, Types::getDataTypeSize(dataType)));
    }
    for (auto i = 0u; i < buildDataInfo.payloadsPosAndType.size(); ++i) {
        auto [pos, dataType] = buildDataInfo.payloadsPosAndType[i];
        if (buildDataInfo.isPayloadsInKeyChunk[i]) {
            tableSchema->appendColumn(std::make_unique<ColumnSchema>(
                false /* is flat */, pos.dataChunkPos, Types::getDataTypeSize(dataType)));
        } else {
            auto isVectorFlat = buildDataInfo.isPayloadsFlat[i];
            tableSchema->appendColumn(
                std::make_unique<ColumnSchema>(!isVectorFlat, pos.dataChunkPos,
                    isVectorFlat ? Types::getDataTypeSize(dataType) :
                                   (uint32_t)sizeof(overflow_value_t)));
        }
    }
    // The prev pointer column.
    tableSchema->appendColumn(std::make_unique<ColumnSchema>(false /* is flat */,
        UINT32_MAX /* For now, we just put UINT32_MAX for prev pointer */,
        Types::getDataTypeSize(INT64)));
    return tableSchema;
}

void HashJoinBuild::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->initEmptyHashTable(
        *context->memoryManager, buildDataInfo.getNumKeys(), populateTableSchema());
}

void HashJoinBuild::initLocalHashTable(
    MemoryManager& memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema) {
    hashTable = std::make_unique<JoinHashTable>(memoryManager, buildDataInfo.getNumKeys(),
        std::make_unique<FactorizedTableSchema>(*tableSchema));
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
            appendVectors();
        }
    }
    // Merge with global hash table once local tuples are all appended.
    sharedState->mergeLocalHashTable(*hashTable);
}

} // namespace processor
} // namespace kuzu
