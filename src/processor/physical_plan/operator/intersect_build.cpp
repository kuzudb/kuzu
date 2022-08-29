#include "src/processor/include/physical_plan/operator/intersect_build.h"

#include <iostream>

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> IntersectBuild::init(ExecutionContext* context) {
    mm = context->memoryManager;
    resultSet = PhysicalOperator::init(context);
    keyVector = resultSet->dataChunks[0]->getValueVector(0);
    payloadVector = resultSet->dataChunks[1]->getValueVector(0);
    return resultSet;
}

void IntersectBuild::append() {
    assert(keyVector->state->isFlat() && !payloadVector->state->isFlat());
    auto key = ((nodeID_t*)keyVector->values)[keyVector->state->getPositionOfCurrIdx()];
    auto slot = sharedHT->getSlot(key.offset);
    appendPayloadToBlock(slot);
}

static void copyFromVectorToBlock(ValueVector* payload, uint8_t* block) {
    auto blockValues = (node_offset_t*)(block);
    auto payloadValues = (nodeID_t*)payload->values;
    for (auto i = 0u; i < payload->state->selVector->selectedSize; i++) {
        blockValues[i] = payloadValues[i].offset;
    }
}

static void sortInBlock(node_offset_t* start, uint32_t num) {
    sort(start, start + num);
}

DataBlock* IntersectBuild::getBlockToAppend(uint32_t numValuesToAppend) {
    assert(numValuesToAppend * sizeof(node_offset_t) < LARGE_PAGE_SIZE);
    if (blocks.empty()) {
        blocks.push_back(make_unique<DataBlock>(mm));
    }
    if (blocks.back()->freeSize < (numValuesToAppend * sizeof(node_offset_t))) {
        blocks.push_back(make_unique<DataBlock>(mm));
    }
    return blocks.back().get();
}

void IntersectBuild::appendPayloadToBlock(IntersectSlot* slot) {
    if (blocks.empty()) {
        blocks.push_back(make_unique<DataBlock>(mm));
    }
    auto numValuesToAppend = payloadVector->state->selVector->selectedSize;
    auto& currentBlockToAppend = blocks[currentBlockIdx];
    if (currentBlockToAppend->freeSize >= (numValuesToAppend * sizeof(node_offset_t))) {
        // Current block has free size.
        auto dataInBlockToAppend = currentBlockToAppend->getData() +
                                   currentBlockToAppend->numTuples * sizeof(node_offset_t);
        copyFromVectorToBlock(payloadVector.get(), dataInBlockToAppend);
        auto listStart = slot->numValues == 0 ? dataInBlockToAppend : slot->data;
        slot->data = listStart;
        slot->numValues += numValuesToAppend;
        sortInBlock((node_offset_t*)listStart, slot->numValues);
        currentBlockToAppend->numTuples += numValuesToAppend;
        currentBlockToAppend->freeSize -= (numValuesToAppend * sizeof(node_offset_t));
    } else {
        // Allocate a new block.
        auto block = make_unique<DataBlock>(mm);
        auto numExistingBytes = slot->numValues * sizeof(node_offset_t);
        assert(numExistingBytes + (numValuesToAppend * sizeof(node_offset_t)) <= LARGE_PAGE_SIZE);
        if (numExistingBytes + (numValuesToAppend * sizeof(node_offset_t)) > LARGE_PAGE_SIZE) {
            cout << "Out of bound for block with size: "
                 << (numExistingBytes + (numValuesToAppend * sizeof(node_offset_t))) << endl;
        }
        if (numExistingBytes > 0) {
            memcpy(block->getData(), slot->data, numExistingBytes);
        }
        auto listStart = block->getData();
        copyFromVectorToBlock(payloadVector.get(), listStart + numExistingBytes);
        slot->data = listStart;
        slot->numValues += numValuesToAppend;
        sortInBlock((node_offset_t*)listStart, slot->numValues);
        block->numTuples += slot->numValues;
        block->freeSize -= (block->numTuples * sizeof(node_offset_t));
        blocks.push_back(move(block));
        currentBlockIdx++;
    }
}

void IntersectBuild::execute(ExecutionContext* context) {
    init(context);
    metrics->executionTime.start();
    while (children[0]->getNextTuples()) {
        append();
    }
    unique_lock xLck{sharedHT->mtx};
    std::move(blocks.begin(), blocks.end(), std::back_inserter(sharedHT->payloadBlocks));
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
