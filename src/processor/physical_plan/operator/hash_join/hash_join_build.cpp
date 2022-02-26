#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace processor {

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
        sharedState->nonKeyDataPosesDataTypes.push_back(vector->dataType);
    }
    // The prev pointer column.
    tableSchema.appendColumn(
        {false /* isUnflat */, UINT32_MAX /* For now, we just put UINT32_MAX for prev pointer */,
            TypeUtils::getDataTypeSize(INT64)});
    factorizedTable = make_unique<FactorizedTable>(context.memoryManager, tableSchema);
    {
        lock_guard<mutex> sharedStateLock(sharedState->hashJoinSharedStateLock);
        if (sharedState->factorizedTable == nullptr) {
            sharedState->factorizedTable =
                make_unique<FactorizedTable>(context.memoryManager, tableSchema);
        }
    }
    return resultSet;
}

void HashJoinBuild::finalize() {
    auto directory_capacity =
        HashTableUtils::nextPowerOfTwo(max(sharedState->factorizedTable->getNumTuples() * 2,
            (DEFAULT_MEMORY_BLOCK_SIZE / sizeof(uint8_t*)) + 1));
    sharedState->hashBitMask = directory_capacity - 1;
    sharedState->htDirectory = context.memoryManager->allocateOSBackedBlock(
        directory_capacity * sizeof(uint8_t*), true /* initializeToZero */);

    auto& tableSchema = sharedState->factorizedTable->getTableSchema();
    hash_t hash;
    auto directory = (uint8_t**)sharedState->htDirectory->data;
    for (auto& tupleBlock : sharedState->factorizedTable->getTupleDataBlocks()) {
        uint8_t* tuple = tupleBlock.data;
        for (auto i = 0u; i < tupleBlock.numEntries; i++) {
            auto nodeId = *(nodeID_t*)tuple;
            Hash::operation<nodeID_t>(nodeId, false /* isNull */, hash);
            auto slotId = hash & sharedState->hashBitMask;
            auto prevPtr = (uint8_t**)(tuple + tableSchema.getNullMapOffset() - sizeof(uint8_t*));
            memcpy((uint8_t*)prevPtr, (void*)&(directory[slotId]), sizeof(uint8_t*));
            directory[slotId] = tuple;
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
    // Merge thread-local state (numEntries, htBlocks, overflowBlocks) with the shared one
    {
        lock_guard<mutex> sharedStateLock(sharedState->hashJoinSharedStateLock);
        sharedState->factorizedTable->merge(*factorizedTable);
    }
    metrics->executionTime.stop();
}
} // namespace processor
} // namespace graphflow
