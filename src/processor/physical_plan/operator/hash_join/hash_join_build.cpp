#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace processor {

void HashJoinSharedState::initEmptyHashTableIfNecessary(
    MemoryManager& memoryManager, TableSchema& tableSchema) {
    lock_guard<mutex> lck(hashJoinSharedStateMutex);
    if (hashTable == nullptr) {
        hashTable = make_unique<JoinHashTable>(memoryManager, 0 /* numTuples */, tableSchema);
    }
}

void HashJoinSharedState::mergeLocalFactorizedTable(FactorizedTable& factorizedTable) {
    lock_guard<mutex> lck(hashJoinSharedStateMutex);
    hashTable->getFactorizedTable()->merge(factorizedTable);
}

shared_ptr<ResultSet> HashJoinBuild::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    TableSchema tableSchema;
    keyDataChunk = resultSet->dataChunks[buildDataInfo.getKeyIDDataChunkPos()];
    auto keyVector = keyDataChunk->valueVectors[buildDataInfo.getKeyIDVectorPos()];
    tableSchema.appendColumn({false /* isUnflat */, buildDataInfo.getKeyIDDataChunkPos(),
        Types::getDataTypeSize(keyVector->dataType)});
    vectorsToAppend.push_back(keyVector);
    for (auto i = 0u; i < buildDataInfo.nonKeyDataPoses.size(); ++i) {
        auto dataChunkPos = buildDataInfo.nonKeyDataPoses[i].dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = buildDataInfo.nonKeyDataPoses[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        auto isVectorFlat = buildDataInfo.isNonKeyDataFlat[i];
        tableSchema.appendColumn({!isVectorFlat, dataChunkPos,
            isVectorFlat ? Types::getDataTypeSize(vector->dataType) :
                           (uint32_t)sizeof(overflow_value_t)});
        vectorsToAppend.push_back(vector);
        sharedState->appendNonKeyDataPosesDataTypes(vector->dataType);
    }
    // The prev pointer column.
    tableSchema.appendColumn(
        {false /* isUnflat */, UINT32_MAX /* For now, we just put UINT32_MAX for prev pointer */,
            Types::getDataTypeSize(INT64)});
    factorizedTable = make_unique<FactorizedTable>(context->memoryManager, tableSchema);
    sharedState->initEmptyHashTableIfNecessary(*context->memoryManager, tableSchema);
    return resultSet;
}

void HashJoinBuild::finalize(ExecutionContext* context) {
    auto hashTable = sharedState->getHashTable();
    auto factorizedTable = hashTable->getFactorizedTable();
    hashTable->allocateHashSlots(factorizedTable->getNumTuples());
    auto& tableSchema = factorizedTable->getTableSchema();
    for (auto& tupleBlock : factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock->getData();
        for (auto i = 0u; i < tupleBlock->numTuples; i++) {
            auto lastSlotEntryInHT = hashTable->insertEntry<nodeID_t>(tuple);
            auto prevPtr = (uint8_t**)(tuple + tableSchema.getNullMapOffset() - sizeof(uint8_t*));
            memcpy((uint8_t*)prevPtr, &lastSlotEntryInHT, sizeof(uint8_t*));
            tuple += tableSchema.getNumBytesPerTuple();
        }
    }
}

void HashJoinBuild::appendVectors() {
    if (keyDataChunk->state->selectedSize == 0) {
        return;
    }
    for (auto i = 0u; i < resultSet->multiplicity; ++i) {
        appendVectorsOnce();
    }
}

void HashJoinBuild::appendVectorsOnce() {
    auto& keyVector = vectorsToAppend[0];
    if (keyVector->state->isFlat()) {
        if (keyVector->isNull(keyVector->state->getPositionOfCurrIdx())) {
            return;
        }
        factorizedTable->append(vectorsToAppend);
    } else {
        if (keyVector->hasNoNullsGuarantee()) {
            factorizedTable->append(vectorsToAppend);
            return;
        }
        // If key vector may contain null value, we have to flatten the key and for each flat key
        // checking whether its null or not.
        for (auto i = 0u; i < keyVector->state->selectedSize; ++i) {
            if (keyVector->isNull(i)) {
                continue;
            }
            keyVector->state->currIdx = i;
            factorizedTable->append(vectorsToAppend);
        }
        keyVector->state->currIdx = -1;
    }
}

void HashJoinBuild::execute(ExecutionContext* context) {
    init(context);
    metrics->executionTime.start();
    // Append thread-local tuples
    while (children[0]->getNextTuples()) {
        appendVectors();
    }
    sharedState->mergeLocalFactorizedTable(*factorizedTable);
    metrics->executionTime.stop();
}
} // namespace processor
} // namespace graphflow
