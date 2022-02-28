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

HashJoinBuild::HashJoinBuild(shared_ptr<HashJoinSharedState> sharedState,
    const BuildDataInfo& buildDataInfo, unique_ptr<PhysicalOperator> child,
    ExecutionContext& context, uint32_t id)
    : Sink{move(child), context, id}, sharedState{move(sharedState)}, buildDataInfo{buildDataInfo} {
}

shared_ptr<ResultSet> HashJoinBuild::initResultSet() {
    resultSet = children[0]->initResultSet();
    TableSchema tableSchema;
    keyDataChunk = resultSet->dataChunks[buildDataInfo.getKeyIDDataChunkPos()];
    auto keyVector = keyDataChunk->valueVectors[buildDataInfo.getKeyIDVectorPos()];
    tableSchema.appendColumn({false /* isUnflat */, buildDataInfo.getKeyIDDataChunkPos(),
        TypeUtils::getDataTypeSize(keyVector->dataType)});
    vectorsToAppend.push_back(keyVector);
    for (auto i = 0u; i < buildDataInfo.nonKeyDataPoses.size(); ++i) {
        auto dataChunkPos = buildDataInfo.nonKeyDataPoses[i].dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = buildDataInfo.nonKeyDataPoses[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        auto isVectorFlat = buildDataInfo.isNonKeyDataFlat[i];
        tableSchema.appendColumn({!isVectorFlat, dataChunkPos,
            isVectorFlat ? TypeUtils::getDataTypeSize(vector->dataType) :
                           sizeof(overflow_value_t)});
        vectorsToAppend.push_back(vector);
        sharedState->appendNonKeyDataPosesDataTypes(vector->dataType);
    }
    // The prev pointer column.
    tableSchema.appendColumn(
        {false /* isUnflat */, UINT32_MAX /* For now, we just put UINT32_MAX for prev pointer */,
            TypeUtils::getDataTypeSize(INT64)});
    factorizedTable = make_unique<FactorizedTable>(context.memoryManager, tableSchema);
    sharedState->initEmptyHashTableIfNecessary(*context.memoryManager, tableSchema);
    return resultSet;
}

void HashJoinBuild::finalize() {
    auto hashTable = sharedState->getHashTable();
    auto factorizedTable = hashTable->getFactorizedTable();
    hashTable->allocateHashSlots(factorizedTable->getNumTuples());
    auto& tableSchema = factorizedTable->getTableSchema();
    for (auto& tupleBlock : factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock.data;
        for (auto i = 0u; i < tupleBlock.numEntries; i++) {
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

void HashJoinBuild::execute() {
    metrics->executionTime.start();
    Sink::execute();
    // Append thread-local tuples
    while (children[0]->getNextTuples()) {
        appendVectors();
    }
    sharedState->mergeLocalFactorizedTable(*factorizedTable);
    metrics->executionTime.stop();
}
} // namespace processor
} // namespace graphflow
