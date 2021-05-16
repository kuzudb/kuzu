#include "src/storage/include/index/hash_index.h"

#include <iostream>

#include "src/common/include/vector/operations/vector_hash_operations.h"

namespace graphflow {
namespace storage {

HashIndex::HashIndex(const string& indexPath, uint64_t indexId, MemoryManager& memoryManager,
    BufferManager& bufferManager, DataType keyType)
    : indexHeader{indexId}, memoryManager{memoryManager}, bufferManager{bufferManager} {
    // entry in slot: hash + key (fixed sized part) + node_offset
    indexHeader.numBytesPerEntry =
        sizeof(uint64_t) + getDataTypeSize(keyType) + sizeof(node_offset_t);
    indexHeader.numBytesPerSlot =
        (indexHeader.numBytesPerEntry * indexHeader.slotCapacity) + NUM_BYTES_PER_SLOT_HEADER;
    indexHeader.numSlotsPerBlock = PAGE_SIZE / indexHeader.numBytesPerSlot;
    initialize(indexPath, indexId);
}

void HashIndex::initialize(const string& indexBasePath, uint64_t indexId) {
    vector<string> filePaths(2); // [indexPrimaryFilePath,indexOverflowFilePath]
    filePaths[0] = FileUtils::joinPath(indexBasePath, (to_string(indexId) + INDEX_FILE_POSTFIX));
    filePaths[1] = FileUtils::joinPath(indexBasePath, OVERFLOW_FILE_NAME + INDEX_FILE_POSTFIX);
    memoryBlocks.resize(2);
    fileHandles.resize(2);
    bool isIndexFileExist = true;
    // Open index files
    for (auto i = 0u; i < 2; i++) {
        int flags = O_RDWR;
        if (!FileUtils::fileExists(filePaths[i])) {
            flags |= O_CREAT;
            isIndexFileExist = false;
        }
        fileHandles[i] = make_unique<FileHandle>(filePaths[i], flags);
        // Cache the first page of primary and overflow index file
        getMemoryBlock(0, i == 1);
    }
    if (isIndexFileExist) {
        deSerIndexHeaderAndOvfPageSet();
    } else {
        // Pre-allocate primary slots so it is consistent with the initialization of IndexHeader
        // that the index starts with level 1 and 2 slots.
        memoryBlocks[0][1] = memoryManager.allocateBlock(PAGE_SIZE, true /* initialize */);
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
void HashIndex::lookup(ValueVector& keys, ValueVector& result) {
    uint8_t* keyData = keys.values;
    auto resultData = (uint64_t*)result.values;
    auto hashes = make_shared<ValueVector>(INT64);
    hashes->state = keys.state;
    VectorHashOperations::Hash(keys, *hashes);
    auto offset = keys.state->isFlat() ? keys.state->getCurrSelectedValuesPos() : 0;
    auto numKeys = keys.state->isFlat() ? 1 : keys.state->numSelectedValues;
    auto slots = calculateSlotIdForHashes(*hashes, offset, numKeys);
    auto numBytesPerKey = keys.getNumBytesPerValue();
    for (auto keyPos = 0u; keyPos < numKeys; keyPos++) {
        auto blockId = getPrimaryBlockIdForSlot(slots[keyPos]);
        auto slotIdInBlock = slots[keyPos] % indexHeader.numSlotsPerBlock;
        uint8_t* expectedKey =
            keyData + (keys.state->selectedValuesPos[keyPos + offset] * numBytesPerKey);
        auto resultValue = lookupKeyInSlot(expectedKey, numBytesPerKey, blockId, slotIdInBlock);
        resultData[keyPos] = resultValue;
        result.nullMask[keyPos] = resultValue == UINT64_MAX;
    }
}

// First visit the primary slot, then loop over all overflow slots until the key is found, or all
// slots are done. Return UINT64_MAX if no key is found.
uint64_t HashIndex::lookupKeyInSlot(
    uint8_t* key, uint64_t numBytesPerKey, uint64_t blockId, uint64_t slotIdInBlock) {
    auto fileHandle = fileHandles[0].get(); // Primary file handle
    while (blockId != 0) {
        const uint8_t* block = bufferManager.pin(*fileHandle, blockId);
        const uint8_t* slot = block + (slotIdInBlock * indexHeader.numBytesPerSlot);
        auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
        for (auto entryPos = 0u; entryPos < slotHeader.numEntries; entryPos++) {
            auto keyInSlot = slot + NUM_BYTES_PER_SLOT_HEADER +
                             (entryPos * indexHeader.numBytesPerEntry) + sizeof(uint64_t);
            if (memcmp(key, keyInSlot, numBytesPerKey) == 0) {
                uint64_t result = *(uint64_t*)(keyInSlot + numBytesPerKey);
                bufferManager.unpin(*fileHandle, blockId);
                return result;
            }
        }
        bufferManager.unpin(*fileHandle, blockId);
        fileHandle = fileHandles[1].get(); // Overflow file handle
        blockId = slotHeader.ovfPageId;
        slotIdInBlock = slotHeader.ovfSlotIdInPage;
    }
    return UINT64_MAX;
}

// Check if key exists in the given slot. Return true if key not exists, else false.
bool HashIndex::keyNotExistInSlot(
    uint8_t* key, uint64_t numBytesPerKey, uint64_t blockId, uint64_t slotIdInBlock) {
    bool isOverflow = false;
    while (blockId != 0) {
        uint8_t* block = getMemoryBlock(blockId, isOverflow);
        uint8_t* slot = block + (slotIdInBlock * indexHeader.numBytesPerSlot);
        auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
        for (auto entryPos = 0u; entryPos < slotHeader.numEntries; entryPos++) {
            uint8_t* keyInSlot = slot + NUM_BYTES_PER_SLOT_HEADER +
                                 (entryPos * indexHeader.numBytesPerEntry) + sizeof(uint64_t);
            if (memcmp(key, keyInSlot, numBytesPerKey) == 0) {
                return false;
            }
        }
        isOverflow = true;
        blockId = slotHeader.ovfPageId;
        slotIdInBlock = slotHeader.ovfSlotIdInPage;
    }
    return true;
}

uint8_t* HashIndex::findEntryToAppendAndUpdateSlotHeader(uint8_t* slot) {
    auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
    while (slotHeader.ovfPageId > 0) {
        uint8_t* slotBlock = getMemoryBlock(slotHeader.ovfPageId, true);
        slot = slotBlock + (slotHeader.ovfSlotIdInPage * indexHeader.numBytesPerSlot);
        slotHeader = deSerSlotHeader(*(uint64_t*)slot);
    }
    if (slotHeader.numEntries == indexHeader.slotCapacity) {
        uint8_t* overflowSlot = findFreeOverflowSlot(slotHeader);
        *(uint64_t*)slot = serSlotHeader(slotHeader);
        slot = overflowSlot;
        slotHeader = deSerSlotHeader(*(uint64_t*)slot);
    }
    uint8_t* entry =
        slot + NUM_BYTES_PER_SLOT_HEADER + slotHeader.numEntries * indexHeader.numBytesPerEntry;
    slotHeader.numEntries++;
    *(uint64_t*)slot = serSlotHeader(slotHeader);
    return entry;
}

void HashIndex::flush() {
    serIndexHeaderAndOvfPageSet();
    for (auto& block : memoryBlocks[0]) {
        fileHandles[0]->writePage(block.second->blockPtr, block.first);
    }
    for (auto& block : memoryBlocks[1]) {
        fileHandles[1]->writePage(block.second->blockPtr, block.first);
    }
}

vector<bool> HashIndex::notExists(ValueVector& keys, ValueVector& hashes) {
    uint8_t* keyData = keys.values;
    auto offset = keys.state->isFlat() ? keys.state->getCurrSelectedValuesPos() : 0;
    auto numKeys = keys.state->isFlat() ? 1 : keys.state->numSelectedValues;
    if (indexHeader.currentNumEntries == 0) {
        vector<bool> notExistsVector(numKeys, true);
        return notExistsVector;
    }
    auto slots = calculateSlotIdForHashes(hashes, offset, numKeys);
    vector<bool> keyNotExists(numKeys);
    auto numBytesPerKey = keys.getNumBytesPerValue();
    for (auto keyPos = 0u; keyPos < numKeys; keyPos++) {
        auto blockId = getPrimaryBlockIdForSlot(slots[keyPos]);
        auto slotIdInBlock = slots[keyPos] % indexHeader.numSlotsPerBlock;
        auto expectedKey =
            keyData + (keys.state->selectedValuesPos[keyPos + offset] * numBytesPerKey);
        keyNotExists[keyPos] =
            keyNotExistInSlot(expectedKey, numBytesPerKey, blockId, slotIdInBlock);
    }
    return keyNotExists;
}

void HashIndex::insertInternal(
    ValueVector& keys, ValueVector& hashes, ValueVector& values, vector<bool>& keyNotExists) {
    auto numBytesPerKey = keys.getNumBytesPerValue();
    auto hashesData = (uint64_t*)hashes.values;
    auto offset = keys.state->isFlat() ? keys.state->getCurrSelectedValuesPos() : 0;
    auto numKeys = keys.state->isFlat() ? 1 : keys.state->numSelectedValues;
    vector<uint64_t> keyPositions(numKeys);
    auto numEntriesToInsert = 0;
    for (auto pos = 0u; pos < numKeys; pos++) {
        keyPositions[numEntriesToInsert] = keys.state->selectedValuesPos[pos + offset];
        // numEntriesToInsert is only incremented when key exists. otherwise keyNotExists[pos] is 0.
        numEntriesToInsert += keyNotExists[pos];
    }
    reserve(numEntriesToInsert + indexHeader.currentNumEntries);
    auto slotIds = calculateSlotIdForHashes(hashes, offset, numKeys);
    for (auto i = 0u; i < numEntriesToInsert; i++) {
        uint8_t* slot = getPrimarySlot(slotIds[i]);
        auto entryInSlot = findEntryToAppendAndUpdateSlotHeader(slot);
        memcpy(entryInSlot, &hashesData[i], sizeof(uint64_t));
        memcpy(entryInSlot + sizeof(uint64_t), keys.values + keyPositions[i] * numBytesPerKey,
            numBytesPerKey);
        memcpy(entryInSlot + sizeof(uint64_t) + numBytesPerKey,
            values.values + keyPositions[i] * numBytesPerKey, sizeof(uint64_t));
    }
    indexHeader.currentNumEntries += numEntriesToInsert;
}

uint8_t* HashIndex::getMemoryBlock(uint64_t blockId, bool isOverflow) {
    auto& memoryBlockMap = memoryBlocks[isOverflow];
    auto fileHandle = fileHandles[isOverflow].get();
    if (memoryBlockMap.find(blockId) == memoryBlockMap.end()) {
        try {
            memoryBlockMap[blockId] = memoryManager.allocateBlock(PAGE_SIZE, true /* initialize */);
        } catch (const invalid_argument& exception) {
            memoryBlocks.clear();
            throw invalid_argument(exception.what());
        }
        if (blockId < fileHandle->numPages) {
            fileHandle->readPage(memoryBlockMap[blockId]->blockPtr, blockId);
        }
    }
    return memoryBlockMap[blockId]->blockPtr;
}

uint8_t* HashIndex::getPrimarySlot(uint64_t slotId) {
    auto blockId = getPrimaryBlockIdForSlot(slotId);
    uint8_t* block = getMemoryBlock(blockId, false);
    auto slotIdInBlock = slotId % indexHeader.numSlotsPerBlock;
    return block + (slotIdInBlock * indexHeader.numBytesPerSlot);
}

void HashIndex::reserve(uint64_t numEntries) {
    while (
        (double)(numEntries) / (double)(indexHeader.slotCapacity * indexHeader.currentNumSlots) >=
        indexHeader.maxLoadFactor) {
        splitSlot();
    }
}

vector<uint64_t> HashIndex::calculateSlotIdForHashes(
    ValueVector& hashes, uint64_t offset, uint64_t numValues) const {
    auto hashesData = (uint64_t*)hashes.values;
    vector<uint64_t> slots(numValues);
    auto hashMask = (1 << indexHeader.currentLevel) - 1;
    auto splitHashMask = (1 << (indexHeader.currentLevel + 1)) - 1;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = hashes.state->selectedValuesPos[i + offset];
        auto slotId = hashesData[pos] & hashMask;
        slots[i] =
            slotId >= indexHeader.nextSplitSlotId ? slotId : (hashesData[pos] & splitHashMask);
    }
    return slots;
}

vector<uint8_t*> HashIndex::getPrimaryAndOverflowSlots(uint64_t slotId) {
    uint8_t* slot = getPrimarySlot(slotId);
    auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
    vector<uint8_t*> slots;
    slots.push_back(slot);
    while (slotHeader.ovfPageId > 0) {
        auto block = getMemoryBlock(slotHeader.ovfPageId, false);
        slot = block + (slotHeader.ovfSlotIdInPage * indexHeader.numBytesPerSlot);
        slotHeader = deSerSlotHeader(*(uint64_t*)slot);
        slots.push_back(slot);
    }
    return slots;
}

uint64_t HashIndex::allocateFreeOverflowPage() {
    uint64_t pageGroupId = 0;
    uint64_t freeOverflowPageId = 0;
    while (freeOverflowPageId == 0) {
        if (pageGroupId >= ovfPagesAllocationBitsets.size()) {
            auto newPageGroupBitset = bitset<NUM_PAGES_PER_PAGE_GROUP>();
            newPageGroupBitset[0] = true;
            pageGroupId = ovfPagesAllocationBitsets.size();
            ovfPagesAllocationBitsets.push_back(newPageGroupBitset);
            freeOverflowPageId = pageGroupId * NUM_PAGES_PER_PAGE_GROUP + 1;
        }
        auto& pageGroupBitset = ovfPagesAllocationBitsets[pageGroupId];
        if (!pageGroupBitset.all()) {
            // skip the first page in each page group
            for (auto pagePos = 1u; pagePos < pageGroupBitset.size(); pagePos++) {
                if (!pageGroupBitset[pagePos]) {
                    freeOverflowPageId = pageGroupId * NUM_PAGES_PER_PAGE_GROUP + pagePos;
                    ovfPagesAllocationBitsets[pageGroupId][pagePos] = true;
                    break;
                }
            }
        } else {
            pageGroupId++;
        }
    }
    return freeOverflowPageId;
}

void HashIndex::splitSlot() {
    auto newSlotId = indexHeader.currentNumSlots + 1;
    auto newSlotBlockId = getPrimaryBlockIdForSlot(newSlotId);
    if (newSlotId % indexHeader.numSlotsPerBlock == 0) {
        // Allocate a new primary index page for the new slot
        memoryBlocks[0][newSlotBlockId] =
            memoryManager.allocateBlock(PAGE_SIZE, true /* initialize */);
    } else {
        getMemoryBlock(newSlotBlockId, false);
    }
    // Find all slots to be splitted, including the primary slot and its overflow slots.
    auto slotsToSplit = getPrimaryAndOverflowSlots(indexHeader.nextSplitSlotId);
    vector<uint64_t> slotIds(indexHeader.slotCapacity);
    auto hashMask = (1 << (indexHeader.currentLevel + 1)) - 1;
    for (auto slotToSplit : slotsToSplit) {
        auto slotToSplitHeader = deSerSlotHeader(*(uint64_t*)slotToSplit);
        auto numEntriesInSlotToSplit = slotToSplitHeader.numEntries;
        // Read hash value for each entry, and re-compute slot ids to be inserted.
        for (auto entryPos = 0u; entryPos < numEntriesInSlotToSplit; entryPos++) {
            auto hash = *(uint64_t*)(slotToSplit + NUM_BYTES_PER_SLOT_HEADER +
                                     (entryPos * indexHeader.numBytesPerEntry));
            slotIds[entryPos] = hash & hashMask;
        }
        slotToSplitHeader.reset();
        *(uint64_t*)slotToSplit = serSlotHeader(slotToSplitHeader);
        for (auto i = 0u; i < numEntriesInSlotToSplit; i++) {
            uint8_t* targetSlot = getPrimarySlot(slotIds[i]);
            auto targetEntry = findEntryToAppendAndUpdateSlotHeader(targetSlot);
            memcpy(targetEntry,
                slotToSplit + NUM_BYTES_PER_SLOT_HEADER + (i * indexHeader.numBytesPerEntry),
                indexHeader.numBytesPerEntry);
        }
    }
    updateIndexHeaderAfterSlotSplit();
}

uint8_t* HashIndex::findFreeOverflowSlot(SlotHeader& prevSlotHeader) {
    uint64_t freeOverflowPageId = 0; // pageId 0 should never be a free overflow page.
    // Check if there are free slots in allocated overflow pages of this index.
    for (auto& page : ovfPagesFreeMap) {
        if (page.second) {
            freeOverflowPageId = page.first;
            break;
        }
    }
    // Find available page to be allocated in the allocation bitset of overflow page groups.
    if (freeOverflowPageId == 0) {
        freeOverflowPageId = allocateFreeOverflowPage();
    }
    prevSlotHeader.ovfPageId = freeOverflowPageId;
    // Find free slot in page.
    uint8_t* block = getMemoryBlock(freeOverflowPageId, true);
    uint8_t* result;
    uint64_t numFreeSlots = 0;
    for (auto slotPos = 0u; slotPos < indexHeader.numSlotsPerBlock; slotPos++) {
        uint8_t* slot = block + (slotPos * indexHeader.numBytesPerSlot);
        auto slotHeader = deSerSlotHeader(*(uint64_t*)slot);
        if (slotHeader.numEntries == (uint8_t)0) {
            numFreeSlots++;
            result = slot;
            prevSlotHeader.ovfSlotIdInPage = slotPos;
        }
    }
    // Add the page into index's overflow pages map.
    ovfPagesFreeMap[freeOverflowPageId] = numFreeSlots > 1;
    return result;
}

uint64_t HashIndex::serSlotHeader(SlotHeader& slotHeader) {
    uint64_t value = slotHeader.ovfPageId;
    value |= ((uint64_t)slotHeader.ovfSlotIdInPage << 32);
    value |= ((uint64_t)slotHeader.numEntries << 56);
    return value;
}

SlotHeader HashIndex::deSerSlotHeader(const uint64_t value) {
    SlotHeader slotHeader;
    slotHeader.numEntries = value >> 56;
    slotHeader.ovfSlotIdInPage = (value >> 32) & OVF_SLOT_ID_MASK;
    slotHeader.ovfPageId = value & OVF_PAGE_ID_MASK;
    return slotHeader;
}

void HashIndex::serIndexHeaderAndOvfPageSet() {
    uint8_t* headerBuffer = memoryBlocks[0][0]->blockPtr;
    auto ovfPageSetSize = (PAGE_SIZE - INDEX_HEADER_SIZE) / sizeof(uint32_t);
    ovfPageSetSize = min(ovfPageSetSize, (uint64_t)ovfPagesFreeMap.size());
    uint32_t ovfPagesList[ovfPageSetSize];
    auto ovfPagesFreeMapItr = ovfPagesFreeMap.begin();
    for (auto i = 0u; i < ovfPageSetSize; i++) {
        auto ovfPageId = ovfPagesFreeMapItr->first;
        ovfPagesList[i] = ovfPagesFreeMapItr->second ? ovfPageId | PAGE_FREE_MASK : ovfPageId;
        ovfPagesFreeMapItr++;
    }
    memcpy(headerBuffer + INDEX_HEADER_SIZE, &ovfPagesList, ovfPageSetSize * sizeof(uint32_t));
    auto leftOvfPagesSize = ovfPagesFreeMap.size() - ovfPageSetSize;
    auto nextBlockIdForOvfPageSet = getPrimaryBlockIdForSlot(indexHeader.currentNumSlots) +
                                    1; // next block/page after current slots
    indexHeader.nextBlockIdForOvfPageSet = leftOvfPagesSize > 0 ? nextBlockIdForOvfPageSet : 0;
    uint8_t* nextBlockForOvfPageSet;
    while (leftOvfPagesSize > 0) {
        nextBlockForOvfPageSet = getMemoryBlock(nextBlockIdForOvfPageSet, false);
        ovfPageSetSize = min((PAGE_SIZE - sizeof(uint64_t)) / sizeof(uint32_t), leftOvfPagesSize);
        uint32_t nextOvfPagesList[ovfPageSetSize];
        for (auto i = 0u; i < ovfPageSetSize; i++) {
            auto ovfPageId = ovfPagesFreeMapItr->first;
            nextOvfPagesList[i] =
                ovfPagesFreeMapItr->second ? ovfPageId | PAGE_FREE_MASK : ovfPageId;
            ovfPagesFreeMapItr++;
        }
        memcpy(nextBlockForOvfPageSet + sizeof(uint64_t), &nextOvfPagesList,
            ovfPageSetSize * sizeof(uint32_t));
        leftOvfPagesSize -= ovfPageSetSize;
        nextBlockIdForOvfPageSet++;
        *(uint64_t*)nextBlockForOvfPageSet = leftOvfPagesSize > 0 ? nextBlockIdForOvfPageSet : 0;
    }
    memcpy(headerBuffer, &indexHeader, INDEX_HEADER_SIZE);
}

void HashIndex::deSerIndexHeaderAndOvfPageSet() {
    uint8_t* headerBuffer = memoryBlocks[0][0]->blockPtr;
    memcpy(&indexHeader, headerBuffer, INDEX_HEADER_SIZE);
    auto ovfPageSetSize = (PAGE_SIZE - INDEX_HEADER_SIZE) / sizeof(uint32_t);
    ovfPagesFreeMap.reserve(ovfPageSetSize);
    uint32_t ovfPagesInHeaderSet[ovfPageSetSize];
    memcpy(
        &ovfPagesInHeaderSet, headerBuffer + INDEX_HEADER_SIZE, ovfPageSetSize * sizeof(uint32_t));
    for (auto i = 0u; i < ovfPageSetSize; i++) {
        auto pageId = ovfPagesInHeaderSet[i] & PAGE_FULL_MASK;
        ovfPagesFreeMap[pageId] = pageId != ovfPagesInHeaderSet[i];
    }
    auto nextBlockIdForOvfPageSet = indexHeader.nextBlockIdForOvfPageSet;
    while (nextBlockIdForOvfPageSet > 0) {
        uint8_t* nextBlock = getMemoryBlock(nextBlockIdForOvfPageSet, true);
        ovfPageSetSize = (PAGE_SIZE - sizeof(uint64_t)) / sizeof(uint32_t);
        uint32_t nextOvfPagesSet[ovfPageSetSize];
        memcpy(&nextOvfPagesSet, nextBlock, ovfPageSetSize * sizeof(uint32_t));
        for (auto i = 0u; i < ovfPageSetSize; i++) {
            auto pageId = nextOvfPagesSet[i] & PAGE_FULL_MASK;
            ovfPagesFreeMap[pageId] = pageId != nextOvfPagesSet[i];
        }
        nextBlockIdForOvfPageSet = *(uint64_t*)nextBlock;
    }
}

} // namespace storage
} // namespace graphflow
