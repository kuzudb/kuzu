#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace processor {

void HashJoinSharedState::initEmptyHashTableIfNecessary(
    MemoryManager& memoryManager, unique_ptr<FactorizedTableSchema> tableSchema) {
    unique_lock lck(hashJoinSharedStateMutex);
    if (hashTable == nullptr) {
        hashTable = make_unique<JoinHashTable>(memoryManager, 0 /* numTuples */, move(tableSchema));
    }
}

void HashJoinSharedState::mergeLocalHashTable(JoinHashTable& localHashTable) {
    unique_lock lck(hashJoinSharedStateMutex);
    hashTable->merge(localHashTable);
}

shared_ptr<ResultSet> HashJoinBuild::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    unique_ptr<FactorizedTableSchema> tableSchema = make_unique<FactorizedTableSchema>();
    keyDataChunk = resultSet->dataChunks[buildDataInfo.getKeyIDDataChunkPos()];
    auto keyVector = keyDataChunk->valueVectors[buildDataInfo.getKeyIDVectorPos()];
    tableSchema->appendColumn(make_unique<ColumnSchema>(false /* is flat */,
        buildDataInfo.getKeyIDDataChunkPos(), Types::getDataTypeSize(keyVector->dataType)));
    vectorsToAppend.push_back(keyVector);
    for (auto i = 0u; i < buildDataInfo.nonKeyDataPoses.size(); ++i) {
        auto dataChunkPos = buildDataInfo.nonKeyDataPoses[i].dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = buildDataInfo.nonKeyDataPoses[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        auto isVectorFlat = buildDataInfo.isNonKeyDataFlat[i];
        if (dataChunkPos == buildDataInfo.getKeyIDDataChunkPos()) {
            tableSchema->appendColumn(make_unique<ColumnSchema>(
                false /* is flat */, dataChunkPos, Types::getDataTypeSize(vector->dataType)));
        } else {
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
    hashTable = make_unique<JoinHashTable>(*context->memoryManager, 0 /* empty table */,
        make_unique<FactorizedTableSchema>(*tableSchema));
    sharedState->initEmptyHashTableIfNecessary(*context->memoryManager, std::move(tableSchema));
    return resultSet;
}

void HashJoinBuild::finalize(ExecutionContext* context) {
    auto numTuples = sharedState->getHashTable()->getNumTuples();
    sharedState->getHashTable()->allocateHashSlots(numTuples);
    sharedState->getHashTable()->buildHashSlots();
}

void HashJoinBuild::appendVectors() {
    auto& keyVector = vectorsToAppend[0];
    auto hasNonNullsAfterDiscarding = NodeIDVector::discardNull(*keyVector);
    if (hasNonNullsAfterDiscarding) {
        hashTable->append(vectorsToAppend);
    }
}

void HashJoinBuild::execute(ExecutionContext* context) {
    init(context);
    metrics->executionTime.start();
    // Append thread-local tuples
    while (children[0]->getNextTuples()) {
        for (auto i = 0u; i < resultSet->multiplicity; ++i) {
            appendVectors();
        }
    }
    // Merge with global hash table once local tuples are all appended.
    sharedState->mergeLocalHashTable(*hashTable);
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
