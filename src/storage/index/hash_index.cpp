#include "src/storage/include/index/hash_index.h"

#include <climits>

#include "src/common/include/vector/operations/vector_hash_operations.h"

namespace graphflow {
namespace storage {

HashIndex::HashIndex(const string& indexPath, uint64_t indexId, MemoryManager& memoryManager,
    BufferManager& bufferManager, OverflowPagesManager& overflowPagesManager, DataType keyType)
    : memoryManager{memoryManager}, bufferManager{bufferManager}, overflowPagesManager{
                                                                      overflowPagesManager} {
    // Entry in slot: hash + key (fixed sized part) + node_offset
    numBytesPerEntry = sizeof(uint64_t) + getDataTypeSize(keyType) + sizeof(node_offset_t);
    numBytesPerSlot = (numBytesPerEntry * indexHeader.slotCapacity) + NUM_BYTES_PER_SLOT_HEADER;
    numSlotsPerPrimaryBlock = PAGE_SIZE / numBytesPerSlot;
    numSlotsPerOverflowBlock = numSlotsPerPrimaryBlock;
    // Num of bytes used for storing the slots bitset in the overflow page to keep track of if each
    // slot is free or not.
    numBytesPerOverflowSlotsBitset = (numSlotsPerOverflowBlock + (CHAR_BIT - 1)) / CHAR_BIT;
    while (
        numBytesPerOverflowSlotsBitset + (numSlotsPerOverflowBlock * numBytesPerSlot) > PAGE_SIZE) {
        numBytesPerOverflowSlotsBitset = (numSlotsPerOverflowBlock + (CHAR_BIT)) / CHAR_BIT;
        numSlotsPerOverflowBlock--;
    }
    initialize(indexPath, indexId);
}

void HashIndex::initialize(const string& indexBasePath, uint64_t indexId) {
    auto filePath = FileUtils::joinPath(indexBasePath, (to_string(indexId) + INDEX_FILE_POSTFIX));
    bool isIndexFileExist = true;
    // Open the index file and create it if not exists
    int flags = O_RDWR;
    if (!FileUtils::fileExists(filePath)) {
        flags |= O_CREAT;
        isIndexFileExist = false;
    }
    fileHandle = make_unique<FileHandle>(filePath, flags);
    // Cache the first page of primary index file
    getMemoryBlock(0);
    if (isIndexFileExist) {
        deSerIndexHeaderAndOverflowPageSet();
    } else {
        // Pre-allocate primary slots so it is consistent with the initialization of IndexHeader
        // that the index starts with level 1 and 2 slots.
        memoryBlocks[1] = memoryManager.allocateBlock(PAGE_SIZE, true /* initialize */);
    }
}

// First compute hashes, then check if keys already exist, finally insert non-existing keys.
// For now, we don't support re-write of keys. Existing keys will not be inserted.
// Return: true, insertion succeeds. false, insertion failed due to that the key already exists.
vector<bool> HashIndex::insert(ValueVector& keys, ValueVector& values) {
    auto hashes = make_shared<ValueVector>(INT64);
    hashes->state = keys.state;
    VectorHashOperations::Hash(keys, *hashes);
    auto keyNotExists = notExists(keys, *hashes);
    insertInternal(keys, *hashes, values, keyNotExists);
    return keyNotExists;
}

// The nullmask of result value vector indicates if the key is found in the index or not.
// nullmask value is true means the key cannot be found in the index.
void HashIndex::lookup(ValueVector& keys, ValueVector& result, BufferManagerMetrics& metrics) {
    uint8_t* keyData = keys.values;
    auto resultData = (uint64_t*)result.values;
    auto hashes = make_shared<ValueVector>(INT64);
    hashes->state = keys.state;
    VectorHashOperations::Hash(keys, *hashes);
    auto offset = keys.state->isFlat() ? keys.state->getCurrSelectedValuesPos() : 0;
    auto numKeys = keys.state->isFlat() ? 1 : keys.state->size;
    auto slotIds = calculateSlotIdsForHashes(*hashes, offset, numKeys);
    auto numBytesPerKey = keys.getNumBytesPerValue();
    for (auto keyPos = 0u; keyPos < numKeys; keyPos++) {
        auto blockId = getPrimaryBlockIdForSlot(slotIds[keyPos]);
        auto slotIdInBlock = slotIds[keyPos] % numSlotsPerPrimaryBlock;
        uint8_t* expectedKey =
            keyData + (keys.state->selectedValuesPos[keyPos + offset] * numBytesPerKey);
        auto resultValue =
            lookupKeyInSlot(expectedKey, numBytesPerKey, blockId, slotIdInBlock, metrics);
        resultData[keyPos] = resultValue;
        result.nullMask[keyPos] = resultValue == UINT64_MAX;
    }
}

// First visit the primary slot, then loop over all overflow slots until the key is found, or all
// slots are done. Return UINT64_MAX if no key is found.
uint64_t HashIndex::lookupKeyInSlot(uint8_t* key, uint64_t numBytesPerKey, uint64_t pageId,
    uint64_t slotIdInBlock, BufferManagerMetrics& metrics) const {
    bool isOverflow = false;
    auto currentFileHandle = fileHandle.get();
    while (pageId != 0) {
        const uint8_t* block = bufferManager.pin(*currentFileHandle, pageId, metrics);
        const uint8_t* slot = block + (isOverflow * numBytesPerOverflowSlotsBitset) +
                              (slotIdInBlock * numBytesPerSlot);
        auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
        for (auto entryPos = 0u; entryPos < slotHeader.numEntries; entryPos++) {
            auto keyInSlot =
                slot + NUM_BYTES_PER_SLOT_HEADER + (entryPos * numBytesPerEntry) + sizeof(uint64_t);
            if (memcmp(key, keyInSlot, numBytesPerKey) == 0) {
                uint64_t result = *(uint64_t*)(keyInSlot + numBytesPerKey);
                bufferManager.unpin(*currentFileHandle, pageId);
                return result;
            }
        }
        bufferManager.unpin(*currentFileHandle, pageId);
        currentFileHandle = overflowPagesManager.fileHandle.get();
        pageId = slotHeader.overflowPageId;
        slotIdInBlock = slotHeader.overflowSlotIdInPage;
        isOverflow = true;
    }
    return UINT64_MAX;
}

uint8_t* HashIndex::findEntryToAppendAndUpdateSlotHeader(uint8_t* slot) {
    auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
    // Find the last overflow page in the chain if exists.
    while (slotHeader.overflowPageId > 0) {
        uint8_t* slotBlock = overflowPagesManager.getMemoryBlock(slotHeader.overflowPageId);
        slot = slotBlock + numBytesPerOverflowSlotsBitset +
               (slotHeader.overflowSlotIdInPage * numBytesPerSlot);
        slotHeader = deSerSlotHeader(*(uint64_t*)slot);
    }
    if (slotHeader.numEntries == indexHeader.slotCapacity) {
        // Allocate a new overflow slot and chain it if the current one is full.
        uint8_t* overflowSlot = findFreeOverflowSlot(slotHeader);
        *(uint64_t*)slot = serSlotHeader(slotHeader);
        slot = overflowSlot;
        slotHeader = deSerSlotHeader(*(uint64_t*)slot);
    }
    uint8_t* entry = slot + NUM_BYTES_PER_SLOT_HEADER + slotHeader.numEntries * numBytesPerEntry;
    slotHeader.numEntries++;
    *(uint64_t*)slot = serSlotHeader(slotHeader);
    return entry;
}

void HashIndex::flush() {
    serIndexHeaderAndOverflowPageSet();
    for (auto& block : memoryBlocks) {
        fileHandle->writePage(block.second->blockPtr, block.first);
    }
    overflowPagesManager.flush();
}

vector<bool> HashIndex::notExists(ValueVector& keys, ValueVector& hashes) {
    uint8_t* keyData = keys.values;
    auto offset = keys.state->isFlat() ? keys.state->getCurrSelectedValuesPos() : 0;
    auto numKeys = keys.state->isFlat() ? 1 : keys.state->size;
    if (indexHeader.currentNumEntries == 0) {
        // The index is empty, all keys not exist in the current index.
        vector<bool> keyNotExists(numKeys, true);
        return keyNotExists;
    }
    auto slotIds = calculateSlotIdsForHashes(hashes, offset, numKeys);
    vector<bool> keyNotExists(numKeys);
    auto numBytesPerKey = keys.getNumBytesPerValue();
    for (auto keyPos = 0u; keyPos < numKeys; keyPos++) {
        auto blockId = getPrimaryBlockIdForSlot(slotIds[keyPos]);
        auto slotIdInBlock = slotIds[keyPos] % numSlotsPerPrimaryBlock;
        auto expectedKey =
            keyData + (keys.state->selectedValuesPos[keyPos + offset] * numBytesPerKey);
        keyNotExists[keyPos] =
            keyNotExistInSlot(expectedKey, numBytesPerKey, blockId, slotIdInBlock);
    }
    return keyNotExists;
}

// Check if key exists in the given slot. Return true if key not exists, else false.
bool HashIndex::keyNotExistInSlot(
    uint8_t* key, uint64_t numBytesPerKey, uint64_t blockId, uint64_t slotIdInBlock) {
    bool isOverflow = false;
    while (blockId != 0) {
        uint8_t* block =
            isOverflow ? overflowPagesManager.getMemoryBlock(blockId) : getMemoryBlock(blockId);
        uint8_t* slot = block + (isOverflow * numBytesPerOverflowSlotsBitset) +
                        (slotIdInBlock * numBytesPerSlot);
        auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
        for (auto entryPos = 0u; entryPos < slotHeader.numEntries; entryPos++) {
            uint8_t* keyInSlot =
                slot + NUM_BYTES_PER_SLOT_HEADER + (entryPos * numBytesPerEntry) + sizeof(uint64_t);
            if (memcmp(key, keyInSlot, numBytesPerKey) == 0) {
                return false;
            }
        }
        isOverflow = true;
        blockId = slotHeader.overflowPageId;
        slotIdInBlock = slotHeader.overflowSlotIdInPage;
    }
    return true;
}

void HashIndex::insertInternal(
    ValueVector& keys, ValueVector& hashes, ValueVector& values, vector<bool>& keyNotExists) {
    auto numBytesPerKey = keys.getNumBytesPerValue();
    auto hashesData = (uint64_t*)hashes.values;
    auto offset = keys.state->isFlat() ? keys.state->getCurrSelectedValuesPos() : 0;
    auto numKeys = keys.state->isFlat() ? 1 : keys.state->size;
    vector<uint64_t> keyPositions(numKeys);
    uint64_t numEntriesToInsert = 0;
    for (auto pos = 0u; pos < numKeys; pos++) {
        keyPositions[numEntriesToInsert] = keys.state->selectedValuesPos[pos + offset];
        // numEntriesToInsert is only incremented when key exists. otherwise keyNotExists[pos] is 0.
        numEntriesToInsert += keyNotExists[pos];
    }
    reserve(numEntriesToInsert + indexHeader.currentNumEntries);
    auto slotIds = calculateSlotIdsForHashes(hashes, offset, numKeys);
    for (auto i = 0u; i < numEntriesToInsert; i++) {
        uint8_t* slot = getPrimarySlot(slotIds[i]);
        uint8_t* entryInSlot = findEntryToAppendAndUpdateSlotHeader(slot);
        memcpy(entryInSlot, &hashesData[i], sizeof(uint64_t));
        memcpy(entryInSlot + sizeof(uint64_t), keys.values + keyPositions[i] * numBytesPerKey,
            numBytesPerKey);
        memcpy(entryInSlot + sizeof(uint64_t) + numBytesPerKey,
            values.values + keyPositions[i] * numBytesPerKey, sizeof(uint64_t));
    }
    indexHeader.currentNumEntries += numEntriesToInsert;
}

uint8_t* HashIndex::getMemoryBlock(uint64_t blockId) {
    if (memoryBlocks.find(blockId) == memoryBlocks.end()) {
        // Allocate a memory block for the page, if failed, clean up allocated memoryBlocks and
        // throw an exception.
        try {
            memoryBlocks[blockId] = memoryManager.allocateBlock(PAGE_SIZE, true /* initialize */);
        } catch (const invalid_argument& exception) {
            memoryBlocks.clear();
            throw invalid_argument(exception.what());
        }
        if (blockId < fileHandle->numPages) {
            // Read from the primary index file if the page exists on disk.
            fileHandle->readPage(memoryBlocks[blockId]->blockPtr, blockId);
        }
    }
    return memoryBlocks[blockId]->blockPtr;
}

uint8_t* HashIndex::getPrimarySlot(uint64_t slotId) {
    auto blockId = getPrimaryBlockIdForSlot(slotId);
    uint8_t* block = getMemoryBlock(blockId);
    auto slotIdInBlock = slotId % numSlotsPerPrimaryBlock;
    return block + (slotIdInBlock * numBytesPerSlot);
}

void HashIndex::reserve(uint64_t numEntries) {
    while (
        (double)(numEntries) / (double)(indexHeader.slotCapacity * indexHeader.currentNumSlots) >=
        indexHeader.maxLoadFactor) {
        split();
    }
}

vector<uint64_t> HashIndex::calculateSlotIdsForHashes(
    ValueVector& hashes, uint64_t offset, uint64_t numValues) const {
    auto hashesData = (uint64_t*)hashes.values;
    vector<uint64_t> slotIds(numValues);
    auto hashMask = (1 << indexHeader.currentLevel) - 1;
    auto splitHashMask = (1 << (indexHeader.currentLevel + 1)) - 1;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = hashes.state->selectedValuesPos[i + offset];
        auto slotId = hashesData[pos] & hashMask;
        slotIds[i] =
            slotId >= indexHeader.nextSplitSlotId ? slotId : (hashesData[pos] & splitHashMask);
    }
    return slotIds;
}

void HashIndex::split() {
    auto newSlotId = indexHeader.currentNumSlots + 1;
    auto newSlotBlockId = getPrimaryBlockIdForSlot(newSlotId);
    if (newSlotId % numSlotsPerPrimaryBlock == 0) {
        // Allocate a new primary index page for the new slot
        memoryBlocks[newSlotBlockId] =
            memoryManager.allocateBlock(PAGE_SIZE, true /* initialize */);
    } else {
        getMemoryBlock(newSlotBlockId);
    }
    uint8_t* slotToSplit = getPrimarySlot(indexHeader.nextSplitSlotId);
    auto slotHeader = deSerSlotHeader(*(uint64_t*)slotToSplit);
    splitASlot(slotToSplit);
    while (slotHeader.overflowPageId > 0) {
        auto overflowPageId = slotHeader.overflowPageId;
        auto overflowSlotIdInPage = slotHeader.overflowSlotIdInPage;
        auto block = overflowPagesManager.getMemoryBlock(overflowPageId);
        slotToSplit =
            block + numBytesPerOverflowSlotsBitset + (overflowSlotIdInPage * numBytesPerSlot);
        slotHeader = deSerSlotHeader(*(uint64_t*)slotToSplit);
        auto updatedSlotHeader = splitASlot(slotToSplit);
        // Update overflowPageHeader for the overflow page
        auto isOverflowSlotsUsed =
            OverflowPagesManager::deSerOverflowPageHeader(block, numSlotsPerOverflowBlock);
        isOverflowSlotsUsed[overflowSlotIdInPage] = updatedSlotHeader.numEntries > 0;
        OverflowPagesManager::serOverflowPageHeader(block, isOverflowSlotsUsed);
        updateOverflowPageFreeStatus(overflowPageId, isOverflowSlotsUsed);
    }
    updateIndexHeaderAfterSlotSplit();
}

SlotHeader HashIndex::splitASlot(uint8_t* slotToSplit) {
    auto slotToSplitHeader = deSerSlotHeader(*(uint64_t*)slotToSplit);
    auto numEntriesInSlotToSplit = slotToSplitHeader.numEntries;
    // Read hash value for each entry, and re-compute slot ids to be inserted.
    vector<uint64_t> slotIds(indexHeader.slotCapacity);
    auto hashMask = (1 << (indexHeader.currentLevel + 1)) - 1;
    for (auto entryPos = 0u; entryPos < numEntriesInSlotToSplit; entryPos++) {
        auto hash =
            *(uint64_t*)(slotToSplit + NUM_BYTES_PER_SLOT_HEADER + (entryPos * numBytesPerEntry));
        slotIds[entryPos] = hash & hashMask;
    }
    slotToSplitHeader.reset();
    *(uint64_t*)slotToSplit = serSlotHeader(slotToSplitHeader);
    for (auto i = 0u; i < numEntriesInSlotToSplit; i++) {
        uint8_t* targetSlot = getPrimarySlot(slotIds[i]);
        auto targetEntry = findEntryToAppendAndUpdateSlotHeader(targetSlot);
        memcpy(targetEntry, slotToSplit + NUM_BYTES_PER_SLOT_HEADER + (i * numBytesPerEntry),
            numBytesPerEntry);
    }
    return slotToSplitHeader;
}

uint8_t* HashIndex::findFreeOverflowSlot(SlotHeader& prevSlotHeader) {
    uint64_t freeOverflowPageId = 0; // pageId 0 should never be a free overflow page.
    // Check if there are free slots in allocated overflow pages of this index.
    for (auto& page : overflowPagesFreeMap) {
        if (page.second) {
            freeOverflowPageId = page.first;
            break;
        }
    }
    // Find an available page to be allocated in the allocation bitset of overflow page groups.
    if (freeOverflowPageId == 0) {
        freeOverflowPageId = overflowPagesManager.allocateFreeOverflowPage();
    }
    prevSlotHeader.overflowPageId = freeOverflowPageId;
    // Find a free slot in the page.
    uint8_t* block = overflowPagesManager.getMemoryBlock(freeOverflowPageId);
    uint8_t* result = nullptr;
    auto isOverflowSlotsUsed =
        OverflowPagesManager::deSerOverflowPageHeader(block, numSlotsPerOverflowBlock);
    for (auto slotPos = 0u; slotPos < numSlotsPerOverflowBlock; slotPos++) {
        if (!isOverflowSlotsUsed[slotPos]) {
            result = block + numBytesPerOverflowSlotsBitset + (slotPos * numBytesPerSlot);
            prevSlotHeader.overflowSlotIdInPage = slotPos;
            isOverflowSlotsUsed[slotPos] = true;
            break;
        }
    }
    OverflowPagesManager::serOverflowPageHeader(block, isOverflowSlotsUsed);
    updateOverflowPageFreeStatus(freeOverflowPageId, isOverflowSlotsUsed);
    return result;
}

void HashIndex::updateOverflowPageFreeStatus(
    uint64_t overflowPageId, vector<bool>& isOverflowSlotsUsed) {
    uint64_t numFreeSlots = 0;
    for (auto isUsed : isOverflowSlotsUsed) {
        numFreeSlots += (1 - isUsed);
    }
    // Add the page into index's overflow pages map.
    overflowPagesFreeMap[overflowPageId] = numFreeSlots > 0;
}

uint64_t HashIndex::serSlotHeader(SlotHeader& slotHeader) {
    uint64_t value = slotHeader.overflowPageId;
    value |= ((uint64_t)slotHeader.overflowSlotIdInPage << 32);
    value |= ((uint64_t)slotHeader.numEntries << 56);
    return value;
}

SlotHeader HashIndex::deSerSlotHeader(uint64_t value) {
    SlotHeader slotHeader;
    slotHeader.numEntries = value >> 56;
    slotHeader.overflowSlotIdInPage = (value >> 32) & OVERFLOW_SLOT_ID_MASK;
    slotHeader.overflowPageId = value & OVERFLOW_PAGE_ID_MASK;
    return slotHeader;
}

void HashIndex::serIndexHeaderAndOverflowPageSet() {
    auto blockIdForOverflowPageSet = 0;
    auto overflowPageSetSize = (PAGE_SIZE - INDEX_HEADER_SIZE) / sizeof(uint32_t);
    overflowPageSetSize = min(overflowPageSetSize, (uint64_t)overflowPagesFreeMap.size());
    uint64_t leftOverflowPagesSize = overflowPagesFreeMap.size();
    auto numSpilledBlocks = 0;
    auto overflowPagesFreeMapItr = overflowPagesFreeMap.begin();
    auto numBytesPerBlockHeader = INDEX_HEADER_SIZE;
    do {
        auto block = getMemoryBlock(blockIdForOverflowPageSet);
        uint32_t overflowPagesList[overflowPageSetSize];
        for (auto i = 0u; i < overflowPageSetSize; i++) {
            auto overflowPageId = overflowPagesFreeMapItr->first;
            overflowPagesList[i] = overflowPagesFreeMapItr->second ?
                                       overflowPageId | OVERFLOW_PAGE_FREE_MASK :
                                       overflowPageId;
            overflowPagesFreeMapItr++;
        }
        memcpy(block + numBytesPerBlockHeader, &overflowPagesList,
            overflowPageSetSize * sizeof(uint32_t));
        numBytesPerBlockHeader = sizeof(uint64_t);
        leftOverflowPagesSize -= overflowPageSetSize;
        overflowPageSetSize =
            min((PAGE_SIZE - sizeof(uint64_t)) / sizeof(uint32_t), leftOverflowPagesSize);
        numSpilledBlocks++;
        blockIdForOverflowPageSet =
            getPrimaryBlockIdForSlot(indexHeader.currentNumSlots) + numSpilledBlocks;
        if (blockIdForOverflowPageSet > 0) {
            *(uint64_t*)block = leftOverflowPagesSize > 0 ? blockIdForOverflowPageSet : 0;
        } else {
            indexHeader.nextBlockIdForOverflowPageSet =
                leftOverflowPagesSize > 0 ? blockIdForOverflowPageSet : 0;
        }
    } while (leftOverflowPagesSize > 0);
    memcpy(getMemoryBlock(0), &indexHeader, INDEX_HEADER_SIZE);
}

void HashIndex::deSerIndexHeaderAndOverflowPageSet() {
    uint8_t* headerBuffer = memoryBlocks[0]->blockPtr;
    memcpy(&indexHeader, headerBuffer, INDEX_HEADER_SIZE);
    auto overflowPageSetSize = (PAGE_SIZE - INDEX_HEADER_SIZE) / sizeof(uint32_t);
    overflowPagesFreeMap.reserve(overflowPageSetSize);
    uint32_t overflowPagesInHeaderSet[overflowPageSetSize];
    memcpy(&overflowPagesInHeaderSet, headerBuffer + INDEX_HEADER_SIZE,
        overflowPageSetSize * sizeof(uint32_t));
    for (auto i = 0u; i < overflowPageSetSize; i++) {
        auto pageId = overflowPagesInHeaderSet[i] & OVERFLOW_PAGE_FULL_MASK;
        overflowPagesFreeMap[pageId] = pageId != overflowPagesInHeaderSet[i];
    }
    auto nextBlockIdForOverflowPageSet = indexHeader.nextBlockIdForOverflowPageSet;
    while (nextBlockIdForOverflowPageSet > 0) {
        uint8_t* nextBlock = getMemoryBlock(nextBlockIdForOverflowPageSet);
        overflowPageSetSize = (PAGE_SIZE - sizeof(uint64_t)) / sizeof(uint32_t);
        uint32_t nextOverflowPagesSet[overflowPageSetSize];
        memcpy(&nextOverflowPagesSet, nextBlock, overflowPageSetSize * sizeof(uint32_t));
        for (auto i = 0u; i < overflowPageSetSize; i++) {
            auto pageId = nextOverflowPagesSet[i] & OVERFLOW_PAGE_FULL_MASK;
            overflowPagesFreeMap[pageId] = pageId != nextOverflowPagesSet[i];
        }
        nextBlockIdForOverflowPageSet = *(uint64_t*)nextBlock;
    }
}

} // namespace storage
} // namespace graphflow
