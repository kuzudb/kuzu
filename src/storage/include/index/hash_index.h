#pragma once

#include <bitset>

#include "src/common/include/configs.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_utils.h"
#include "src/storage/include/index/overflow_pages_manager.h"
#include "src/storage/include/memory_manager.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace storage {

constexpr uint64_t NUM_BYTES_PER_SLOT_HEADER = 8;
// Masks used in overflow page set to get/set if the overflow page has free slots or not.
constexpr uint32_t OVERFLOW_PAGE_FREE_MASK = 1u << 31;
constexpr uint32_t OVERFLOW_PAGE_FULL_MASK = (1u << 31) - 1;
// Masks used in slotHeader to get/set the overflow page id and the overflow slot id in page.
constexpr uint64_t OVERFLOW_PAGE_ID_MASK = (1ul << 32) - 1;
constexpr uint32_t OVERFLOW_SLOT_ID_MASK = (1u << 24) - 1;
const string INDEX_FILE_POSTFIX = ".index";

struct HashIndexHeader {
public:
    explicit HashIndexHeader()
        : slotCapacity{HashIndexConfig::SLOT_CAPACITY}, currentNumSlots{2}, currentNumEntries{0},
          maxLoadFactor{HashIndexConfig::MAX_LOAD_FACTOR}, currentLevel{1}, nextSplitSlotId{0},
          nextBlockIdForOverflowPageSet{0} {}

public:
    uint64_t slotCapacity;
    uint64_t currentNumSlots;
    uint64_t currentNumEntries;
    double maxLoadFactor;
    uint64_t currentLevel;
    uint64_t nextSplitSlotId;
    uint64_t nextBlockIdForOverflowPageSet;
};

constexpr uint64_t INDEX_HEADER_SIZE = sizeof(HashIndexHeader);

struct SlotHeader {
public:
    SlotHeader() : numEntries{0}, overflowPageId{0}, overflowSlotIdInPage{0} {}

    void reset() {
        numEntries = 0;
        overflowPageId = 0;
        overflowSlotIdInPage = 0;
    }

public:
    uint8_t numEntries;
    uint32_t overflowPageId;
    uint32_t overflowSlotIdInPage;
};

// Each index has: 1) its own primary index file; and 2) an overflow index file that is shared with
// all indexes.
// The primary index file contains all primary slots and header information.
// Primary index header is the first page, PAGE-0, of the primary index file:
// HashIndexHeader is stored at the beginning of the header page.
// Remaining bytes in the page are used to store overflow page set, which records a set of ids of
// overflow pages allocated to this index. Inside which, each item is a uint32_t value, in which the
// first bit indicates if the page has free slots or not, other bits constitute the page id in
// overflow index file. If the overflow page set is too large to be stored in the first page, we add
// new spill pages at the bottom of the primary index file.
// `nextBlockIdForOverflowPageSet` field in the `HashIndexHeader` stores the page id of the first
// spill page, which also contains a `nextBlockIdForOverflowPageSet` value at the beginning of the
// page. See methods: `serIndexHeaderAndOverflowPageSet`, `deSerIndexHeaderAndOverflowPageSet`.
//
// Slot and entry:
// Each slot consists of a slot header and several entries.
// SLOT_HEADER(numEntries, overflowSlotId) [1 byte: numEntries, 3 bytes: overflowSlotIdInPage, 4
// bytes: overflowSlotId]
// See methods: `serSlotHeader`, `deSerSlotHeader`
// ENTRY: [hash, key (fixed sized part), node_offset]

// -----------------------
// | PAGE 0 (HEADER)     |
// -----------------------  <-|
// | PAGE 1 (slots)      |    |
// -----------------------    |
// | ... (slots) ...     |    | HASH SLOTS
// -----------------------    |
// | PAGE n (slots)      |    |
// -----------------------  <-|
// | OVF_SET SPILL PAGE 1|    |
// -----------------------    |
// | ... ...             |    |
// -----------------------    |
//
// The overflow index file is shared by all indexes.
// It is divided into page groups. Each group contains as many pages as there are bits in 1 page,
// i.e., NUM_BYTES_PER_PAGE*8. This is because that inside each page group, the first page is the
// bitset page, which uses one bit to record if each page in the group is available for allocation
// to an index or not. (cached in `overflowPagesAllocationBitsets`). A page is unavailable in the
// following cases: (1) the first page is the bitset page, so it is always unavailable; (2) the page
// is already allocated to an index.
//
// -------------------  <-|
// | PAGE 0 (BITSET) |    |
// -------------------    |
// | PAGE 1          |    |
// -------------------    |
// | ...         ... |    | PAGE GROUP 0
// -------------------    |
// | PAGE n          |    |
// -------------------  <-|
// | PAGE n+1        |  <-|
// --------------------   | PAGE GROUP 1
// | ... ...         |    |
// --------------------   |
// | ... ...         |    |

class HashIndex {

public:
    HashIndex(const string& indexPath, uint64_t indexId, MemoryManager& memoryManager,
        BufferManager& bufferManager, OverflowPagesManager& overflowPagesManager, DataType keyType);

public:
    vector<bool> insert(ValueVector& keys, ValueVector& values);
    void lookup(ValueVector& keys, ValueVector& result, BufferManagerMetrics& metrics);
    // Reserves space for at least the specified number of elements.
    void reserve(uint64_t numEntries);
    void flush();

private:
    void initialize(const string& indexDir, uint64_t indexId);
    vector<bool> notExists(ValueVector& keys, ValueVector& hashes);
    void insertInternal(
        ValueVector& keys, ValueVector& hashes, ValueVector& values, vector<bool>& keyNotExists);
    uint8_t* getMemoryBlock(uint64_t blockId);
    uint8_t* getPrimarySlot(uint64_t slotId);
    vector<uint64_t> calculateSlotIdsForHashes(
        ValueVector& hashes, uint64_t offset, uint64_t numValues) const;
    void split();
    SlotHeader splitASlot(uint8_t* slotToSplit);

    uint8_t* findFreeOverflowSlot(SlotHeader& prevSlotHeader);

    uint64_t lookupKeyInSlot(uint8_t* key, uint64_t numBytesPerKey, uint64_t pageId,
        uint64_t slotIdInPage, BufferManagerMetrics& metrics) const;
    bool keyNotExistInSlot(
        uint8_t* key, uint64_t numBytesPerKey, uint64_t blockId, uint64_t slotIdInBlock);

    uint8_t* findEntryToAppendAndUpdateSlotHeader(uint8_t* slot);

    static uint64_t serSlotHeader(SlotHeader& slotHeader);
    static SlotHeader deSerSlotHeader(uint64_t slotHeader);
    void serIndexHeaderAndOverflowPageSet();
    void deSerIndexHeaderAndOverflowPageSet();

    void updateOverflowPageFreeStatus(uint64_t overflowPageId, vector<bool>& isOverflowSlotsUsed);

    inline uint64_t getPrimaryBlockIdForSlot(uint64_t slotId) const {
        return (slotId / numSlotsPerPrimaryBlock) + 1;
    }

    inline void updateIndexHeaderAfterSlotSplit() {
        if (indexHeader.nextSplitSlotId < (uint64_t)((1 << indexHeader.currentLevel) - 1)) {
            indexHeader.nextSplitSlotId++;
        } else {
            indexHeader.nextSplitSlotId = 0;
            indexHeader.currentLevel++;
        }
        indexHeader.currentNumSlots++;
    };

private:
    HashIndexHeader indexHeader;
    MemoryManager& memoryManager;
    BufferManager& bufferManager;
    OverflowPagesManager& overflowPagesManager;

    uint64_t numBytesPerEntry;
    uint64_t numBytesPerSlot;
    uint64_t numSlotsPerPrimaryBlock;
    uint64_t numSlotsPerOverflowBlock;
    uint64_t numBytesPerOverflowSlotsBitset;

    // Map: pageId(blockId) -> hasFreeSlot?
    // The map caches all overflow pages allocated for this index.
    unordered_map<uint32_t, bool> overflowPagesFreeMap;
    unique_ptr<FileHandle> fileHandle;
    unordered_map<uint64_t, unique_ptr<BlockHandle>> memoryBlocks;
};
} // namespace storage
} // namespace graphflow
