#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"

namespace graphflow {
namespace processor {

static uint64_t nextPowerOfTwo(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

HashJoinBuild::HashJoinBuild(shared_ptr<HashJoinSharedState> sharedState,
    const BuildDataInfo& buildDataInfo, unique_ptr<PhysicalOperator> child,
    ExecutionContext& context, uint32_t id)
    : Sink{move(child), context, id}, sharedState{move(sharedState)}, buildDataInfo{buildDataInfo} {
}

shared_ptr<ResultSet> HashJoinBuild::initResultSet() {
    resultSet = children[0]->initResultSet();
    RowLayout rowLayout;
    keyDataChunk = resultSet->dataChunks[buildDataInfo.getKeyIDDataChunkPos()];
    auto keyVector = keyDataChunk->valueVectors[buildDataInfo.getKeyIDVectorPos()];
    rowLayout.appendField(
        {keyVector->dataType, keyVector->getNumBytesPerValue(), false /* isVectorOverflow */});
    vectorsToAppend.push_back(keyVector);
    for (auto i = 0u; i < buildDataInfo.nonKeyDataPoses.size(); ++i) {
        auto dataChunkPos = buildDataInfo.nonKeyDataPoses[i].dataChunkPos;
        auto dataChunk = resultSet->dataChunks[dataChunkPos];
        auto vectorPos = buildDataInfo.nonKeyDataPoses[i].valueVectorPos;
        auto vector = dataChunk->valueVectors[vectorPos];
        auto isVectorFlat = buildDataInfo.isNonKeyDataFlat[i];
        auto numBytesForField =
            isVectorFlat ? vector->getNumBytesPerValue() : sizeof(overflow_value_t);
        rowLayout.appendField({vector->dataType, numBytesForField, !isVectorFlat});
        vectorsToAppend.push_back(vector);
    }
    // The prev pointer field.
    rowLayout.appendField({INT64, sizeof(uint8_t*), false /* isVectorOverflow */});
    rowLayout.initialize();
    rowCollection = make_unique<RowCollection>(*context.memoryManager, rowLayout);
    {
        lock_guard<mutex> sharedStateLock(sharedState->hashJoinSharedStateLock);
        if (sharedState->rowCollection == nullptr) {
            sharedState->rowCollection =
                make_unique<RowCollection>(*context.memoryManager, rowLayout);
        }
    }
    return resultSet;
}

void HashJoinBuild::finalize() {
    auto directory_capacity = nextPowerOfTwo(max(sharedState->rowCollection->getNumRows() * 2,
        (DEFAULT_MEMORY_BLOCK_SIZE / sizeof(uint8_t*)) + 1));
    sharedState->hashBitMask = directory_capacity - 1;
    sharedState->htDirectory = context.memoryManager->allocateBlock(
        directory_capacity * sizeof(uint8_t*), true /* initializeToZero */);

    auto& layout = sharedState->rowCollection->getLayout();
    hash_t hash;
    auto directory = (uint8_t**)sharedState->htDirectory->data;
    for (auto& rowBlock : sharedState->rowCollection->getRowDataBlocks()) {
        uint8_t* row = rowBlock.data;
        for (auto i = 0u; i < rowBlock.numEntries; i++) {
            auto nodeId = *(nodeID_t*)row;
            Hash::operation<nodeID_t>(nodeId, false /* isNull */, hash);
            auto slotId = hash & sharedState->hashBitMask;
            auto prevPtr = (uint8_t**)(row + layout.getNullMapOffset() - sizeof(uint8_t*));
            memcpy((uint8_t*)prevPtr, (void*)&(directory[slotId]), sizeof(uint8_t*));
            directory[slotId] = row;
            row += layout.numBytesPerRow;
        }
    }
}

void HashJoinBuild::appendResultSet() {
    if (keyDataChunk->state->selectedSize == 0) {
        return;
    }
    auto& keyVector = vectorsToAppend[0];
    if (keyVector->state->isFlat()) {
        if (keyVector->isNull(keyVector->state->getPositionOfCurrIdx())) {
            return;
        }
        rowCollection->append(vectorsToAppend, 1 /* numTuplesToAppend */);
    } else {
        if (keyVector->hasNoNullsGuarantee()) {
            rowCollection->append(vectorsToAppend, keyVector->state->selectedSize);
            return;
        }
        // If key vector may contain null value, we have to flatten the key and for each flat key
        // checking whether its null or not.
        for (auto i = 0u; i < keyVector->state->selectedSize; ++i) {
            if (keyVector->isNull(i)) {
                continue;
            }
            keyVector->state->currIdx = i;
            rowCollection->append(vectorsToAppend, 1 /* numTuplesToAppend */);
        }
        keyVector->state->currIdx = -1;
    }
}

void HashJoinBuild::execute() {
    metrics->executionTime.start();
    Sink::execute();
    // Append thread-local tuples
    while (children[0]->getNextTuples()) {
        appendResultSet();
    }
    // Merge thread-local state (numEntries, htBlocks, overflowBlocks) with the shared one
    {
        lock_guard<mutex> sharedStateLock(sharedState->hashJoinSharedStateLock);
        sharedState->rowCollection->merge(move(rowCollection));
    }
    metrics->executionTime.stop();
}
} // namespace processor
} // namespace graphflow
