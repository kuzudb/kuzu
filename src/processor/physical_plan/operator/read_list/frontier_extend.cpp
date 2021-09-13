#include "src/processor/include/physical_plan/operator/read_list/frontier_extend.h"

#include <omp.h>

namespace graphflow {
namespace processor {

static uint64_t getNextPowerOfTwo(uint64_t value);

FrontierExtend::FrontierExtend(const DataPos& inDataPos, const DataPos& outDataPos, AdjLists* lists,
    label_t outNodeIDVectorLabel, uint64_t lowerBound, uint64_t upperBound,
    unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
    : ReadList{inDataPos, outDataPos, lists, move(prevOperator), context, id,
          true /* is adj list */},
      startLayer{lowerBound}, endLayer{upperBound}, outNodeIDVectorLabel{outNodeIDVectorLabel} {
    operatorType = FRONTIER_EXTEND;
    uint64_t maxNumThreads = omp_get_max_threads();
    vectors.reserve(maxNumThreads);
    largeListHandles.reserve(maxNumThreads);
    for (auto i = 0u; i < maxNumThreads; i++) {
        vectors.push_back(make_shared<ValueVector>(context.memoryManager, NODE));
        vectors[i]->state = make_shared<DataChunkState>(DEFAULT_VECTOR_CAPACITY);
        largeListHandles.push_back(make_unique<LargeListHandle>(true /* is adj list handle */));
        largeListHandles[i]->setListSyncState(make_shared<ListSyncState>());
    }
    frontierPerLayer.reserve(upperBound + 1);
    threadLocalFrontierPerLayer.reserve(upperBound);
    for (auto i = 0u; i < upperBound; i++) {
        frontierPerLayer.push_back(nullptr);
        threadLocalFrontierPerLayer.emplace_back();
        for (auto j = 0u; j < maxNumThreads; j++) {
            threadLocalFrontierPerLayer[i].push_back(nullptr);
        }
    }
    frontierPerLayer.push_back(nullptr);
    currOutputPos.hasMoreTuplesToProduce = false;
}

void FrontierExtend::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    ReadList::initResultSet(resultSet);
    outValueVector = make_shared<ValueVector>(context.memoryManager, NODE);
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    outValueVector->state->initMultiplicity();
    this->resultSet->insert(outDataPos.dataChunkPos, make_shared<ListSyncState>());
}

void FrontierExtend::getNextTuples() {
    metrics->executionTime.start();
    if (currOutputPos.hasMoreTuplesToProduce) {
        produceOutputTuples();
        metrics->executionTime.stop();
        return;
    }
    do {
        prevOperator->getNextTuples();
        if (inValueVector->state->selectedSize == 0) {
            outDataChunk->state->selectedSize = 0;
            metrics->executionTime.stop();
            return;
        }
    } while (computeFrontiers());
    currOutputPos.reset(startLayer - 1);
    produceOutputTuples();
    metrics->executionTime.stop();
}

bool FrontierExtend::computeFrontiers() {
    frontierPerLayer[0] = createInitialFrontierSet();
    for (auto layer = 0u; layer < endLayer; layer++) {
        if (frontierPerLayer[layer] == nullptr) {
            break;
        }
        extendToThreadLocalFrontiers(layer);
        createGlobalFrontierFromThreadLocalFrontiers(layer);
    }
    return frontierPerLayer[startLayer - 1] == nullptr;
}

void FrontierExtend::extendToThreadLocalFrontiers(uint64_t layer) {
    if (frontierPerLayer[layer] == nullptr) {
        frontierPerLayer[layer + 1] = nullptr;
        return;
    }
    for (auto blockIdx = 0u; blockIdx < frontierPerLayer[layer]->hashTableBlocks.size();
         blockIdx++) {
        auto mainBlock = frontierPerLayer[layer]->hashTableBlocks[blockIdx];
        std::atomic<uint64_t> currSlot{0u};
#pragma omp parallel
        {
            auto threadId = omp_get_thread_num();
            threadLocalFrontierPerLayer[layer][threadId] = createFrontierBag();
            uint64_t slotToExtendFrom;
            while (true) {
                slotToExtendFrom = currSlot++;
                if (slotToExtendFrom >= NUM_SLOTS_PER_BLOCK_SET) {
                    break;
                }
                if (mainBlock[slotToExtendFrom].hasValue) {
                    auto slot = &mainBlock[slotToExtendFrom];
                    while (slot) {
                        do {
                            // Warning: we use non-thread-safe metric inside openmp parallelization
                            // metrics won't be exactly but hopefully good enough.
                            lists->readValues(slot->nodeOffset, vectors[threadId],
                                largeListHandles[threadId], *metrics->bufferManagerMetrics);
                            threadLocalFrontierPerLayer[layer][threadId]->append(
                                *(vectors[threadId].get()), slot->multiplicity);
                        } while (largeListHandles[threadId]->hasMoreToRead());
                        slot = slot->next;
                    }
                }
            }
        }
    }
}

void FrontierExtend::createGlobalFrontierFromThreadLocalFrontiers(uint64_t layer) {
    frontierPerLayer[layer + 1] = makeFrontierSet(layer);
    for (auto threadId = 0; threadId < omp_get_max_threads(); threadId++) {
        // ensure the omp thread with given Id created a local frontier.
        if (threadLocalFrontierPerLayer[layer][threadId] == nullptr) {
            continue;
        }
        auto& localFrontier = threadLocalFrontierPerLayer[layer][threadId];
        std::atomic<uint64_t> currSlot{0u};
#pragma omp parallel
        {
            uint64_t slotToInsert;
            while (true) {
                // We expect many nodes per slot and hence each omp thread grabs a slot at a time.
                slotToInsert = currSlot++;
                if (slotToInsert > NUM_SLOTS_BAG) {
                    break;
                }
                // We do not synchronize as each slot from the localFrontier maps to a unique set of
                // slots in the outout layer frontier.
                frontierPerLayer[layer + 1]->insert(*localFrontier, slotToInsert);
            }
        }
        delete localFrontier;
        localFrontier = nullptr;
    }
}

void FrontierExtend::produceOutputTuples() {
    auto outNodeIDs = (node_offset_t*)outValueVector->values;
    outValueVector->state->selectedSize = 0;
    while (currOutputPos.layer < endLayer) {
        auto frontier = frontierPerLayer[currOutputPos.layer];
        if (frontier == nullptr) {
            break;
        }
        while (currOutputPos.blockIdx < frontier->hashTableBlocks.size()) {
            auto mainBlock = frontier->hashTableBlocks[currOutputPos.blockIdx];
            auto maxSlot = NUM_SLOTS_PER_BLOCK_SET;
            while (outValueVector->state->selectedSize < DEFAULT_VECTOR_CAPACITY &&
                   currOutputPos.slot < maxSlot) {
                if (mainBlock[currOutputPos.slot].hasValue) {
                    auto slot = &mainBlock[currOutputPos.slot];
                    while (slot) {
                        outNodeIDs[outValueVector->state->selectedSize] = slot->nodeOffset;
                        outValueVector->state->multiplicity[outValueVector->state->selectedSize++] =
                            slot->multiplicity;
                        slot = slot->next;
                        if (outValueVector->state->selectedSize == DEFAULT_VECTOR_CAPACITY) {
                            currOutputPos.hasMoreTuplesToProduce = true;
                            return;
                        }
                    }
                }
                currOutputPos.slot++;
            }
            currOutputPos.slot = 0u;
            currOutputPos.blockIdx++;
        }
        currOutputPos.blockIdx = 0u;
        currOutputPos.layer++;
    }
    for (auto& frontier : frontierPerLayer) {
        delete frontier;
        frontier = nullptr;
    }
    metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
}

FrontierSet* FrontierExtend::createInitialFrontierSet() {
    // We assume the inValueVector to be flat similar to AdjListExtend's assumption.
    auto pos = inValueVector->state->getPositionOfCurrIdx();
    auto nodeOffset = inValueVector->readNodeOffset(pos);
    auto numSlots = getNextPowerOfTwo(lists->getNumElementsInList(nodeOffset));
    if (numSlots == 0) {
        return nullptr;
    }
    auto frontier = new FrontierSet();
    frontier->setMemoryManager(context.memoryManager);
    frontier->initHashTable(numSlots);
    do {
        lists->readValues(
            nodeOffset, vectors[0], largeListHandles[0], *metrics->bufferManagerMetrics);
        frontier->insert(*vectors[0]);
    } while (largeListHandles[0]->hasMoreToRead());
    return frontier;
}

FrontierSet* FrontierExtend::makeFrontierSet(uint64_t layer) {
    auto numNodeOffsets = 0u;
    for (auto& localFrontier : threadLocalFrontierPerLayer[layer]) {
        if (localFrontier == nullptr) {
            continue;
        }
        numNodeOffsets += localFrontier->size;
    }
    auto numSlots = getNextPowerOfTwo(numNodeOffsets);
    if (numSlots == 0) {
        return nullptr;
    }
    auto frontier = new FrontierSet();
    frontier->setMemoryManager(context.memoryManager);
    frontier->initHashTable(numSlots < NUM_SLOTS_BAG ? NUM_SLOTS_BAG : numSlots);
    return frontier;
}

FrontierBag* FrontierExtend::createFrontierBag() {
    auto frontierBag = new FrontierBag();
    frontierBag->setMemoryManager(context.memoryManager);
    frontierBag->initHashTable();
    return frontierBag;
}

unique_ptr<PhysicalOperator> FrontierExtend::clone() {
    return make_unique<FrontierExtend>(inDataPos, outDataPos, (AdjLists*)lists,
        outNodeIDVectorLabel, startLayer, endLayer, prevOperator->clone(), context, id);
}

static uint64_t getNextPowerOfTwo(uint64_t value) {
    value--;
    value |= value >> 1u;
    value |= value >> 2u;
    value |= value >> 4u;
    value |= value >> 8u;
    value |= value >> 16u;
    value++;
    return value;
}

} // namespace processor
} // namespace graphflow
