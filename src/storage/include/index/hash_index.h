#pragma once

#include <bitset>
#include <climits>

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/memory_manager.h"
#include "src/storage/include/storage_structure/overflow_pages.h"

using namespace graphflow::common;
using namespace graphflow::loader;

namespace graphflow {
namespace storage {

/**
 * Each index is stored in a single file that has 3 components in it.
 *
 * 1. HashIndexHeader is stored in the first page (pageId 0) of the file. It contains the current
 * state of the hash tables along with certain metrics like the number of pages used etc. Remaining
 * bytes of the pageId 0 are left unused as of now.
 *
 * 2. Primary pages are set of pages that contains all primary slots in the hashIndex. A queried
 * key is mapped to one of the slots based on its hash value and is either stored or looked up
 * just in that slot and any chained overflow slots chained to the primary slot.
 *
 * Slot:
 * Each slot consists of a slot header and several entries. The max number of entries in slot is
 * given by HashIndexConfig::SLOT_CAPACITY. The size of the slot is given by (sizeof(SlotHeader) +
 * (SLOT_CAPACITY * sizeof(Entry)).
 *
 * Entry: [key (fixed sized part), node_offset]
 *
 * 3. Overflow pages are set of pages that holds overflow slots. Overflow slots are used to store
 * entries that comes to the designated primary slot that has already been filled to the capacity.
 * Several overflow slots can be chained after the single primary slot as a singly linked link-list.
 * Each slot's SlotHeader has information about the next overflow slot in the chain and also the
 * number of filled entries in that slot.
 *
 * Layout of the file:
 *
 *       ---------------------
 *  0    | PAGE 0 (HEADER)   |
 *       ---------------------
 *  1    | PAGE 1 (SLOT)     |  <-|
 *       ---------------------    |
 *  2    | PAGE 2 (SLOT)     |    |
 *       ---------------------    |  PRIMARY PAGES
 *               ...              |
 *       ---------------------    |
 *  n    | PAGE n (SLOT)     |  <-|
 *       ---------------------
 *  n+1  | PAGE n+1 (SLOT)   |  <-|
 *       ---------------------    |
 *  n+2  | PAGE n+2 (SLOT)   |    |
 *       ---------------------    |  OVERFLOW PAGES
 *               ...              |
 *       ---------------------    |
 *  n+m  | PAGE n+m (SLOT)   |  <-|
 *       ---------------------
 *
 *  Information about the number of primary pages, primary slots, overflow pages and overflow
 *  slots are stored in the HashIndexHeader.
 *
 *  Mode Of Operations:
 *
 *  WRITE_ONLY MODE: The hashIndex is initially opened in the write mode in which only the
 * insertions are supported. In here, all the page allocations are happen in memory which needs to
 * be saved to disk to persist. Also for performance reasons and based on our use case, we support
 *  `bulkReserve` operation to fix the hashIndex capacity in the beginning before any insertions
 *  are made. This is done by means of calculating the number of primary slots and pages are needed
 *  and pre-allocating them.  In lieu of this, we do not support changing the capacity dynamically
 *  by rehashing and splitting of slots. With this scenario, the hashIndex can still insert entries
 *  more than its capacity but these entries will land in chained overflow slots leading to
 *  degraded performance in insertions as well as in look ups.
 *
 *  READ_ONLY MODE: The hashIndex can be opened in the read mode by supplying it with the name of
 * already existing file that was previously flushed in the write mode. Lookups happen by reading
 *  arbitrary number of required pages to reach the required slot and iterating it to find the
 *  required value. In this mode, all pages are not kept in memory but rather are made accessible
 *  by our Buffer manager.
 *
 *  */

struct SlotHeader {
public:
    SlotHeader() : numEntries{0}, nextOvfSlotId{-1u} {}

    void reset() {
        numEntries = 0;
        nextOvfSlotId = 0;
    }

public:
    uint16_t numEntries;
    uint64_t nextOvfSlotId;
};

struct HashIndexHeader {
    friend class HashIndex;

private:
    HashIndexHeader() = default;

    explicit HashIndexHeader(DataType keyDataType);

    inline void incrementLevel();

    // Constants
    uint64_t numBytesPerEntry{0};
    uint64_t numBytesPerSlot{0};
    uint64_t numSlotsPerPage{0};
    uint64_t slotCapacity{HashIndexConfig::SLOT_CAPACITY};

    uint64_t numEntries{0};
    uint64_t numPrimarySlots{2};
    uint64_t numPrimaryPages{1};
    uint64_t numOvfSlots{1};
    uint64_t numOvfPages{1};

    uint64_t currentLevel{1};
    uint64_t levelHashMask{1};
    uint64_t higherLevelHashMask{3};
    uint64_t nextSplitSlotId{0};

    DataType keyDataType;
};

using hash_function_t = std::function<hash_t(uint8_t*)>;
using insert_function_t =
    std::function<void(uint8_t*, uint8_t*, InMemOverflowPages*, PageByteCursor*)>;
using equals_in_write_function_t =
    std::function<bool(const uint8_t*, const uint8_t*, const InMemOverflowPages*)>;
using equals_in_read_function_t =
    std::function<bool(const uint8_t*, const uint8_t*, OverflowPages*)>;

class HashIndex {

    enum HashIndexMode { WRITE_ONLY, READ_ONLY };

public:
    HashIndex(const string& fName, DataType keyDataType, MemoryManager* memoryManager);

    HashIndex(const string& fName, BufferManager* bufferManager, bool isInMemory);

    ~HashIndex();

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries);

    bool insert(uint8_t* key, node_offset_t value);

    bool lookup(uint8_t* key, node_offset_t& result);

    void saveToDisk();

private:
    bool notExistsInSlot(uint8_t* slot, uint8_t* key);
    void putNewEntryInSlotAndUpdateHeader(uint8_t* slot, uint8_t* key, node_offset_t value);
    uint64_t reserveOvfSlot();

    bool lookupInSlot(const uint8_t* slot, uint8_t* key, node_offset_t& result) const;

    inline uint64_t calculateSlotIdForHash(hash_t hash) const;

    inline uint64_t getPageIdForSlot(uint64_t slotId) const {
        return slotId / indexHeader.numSlotsPerPage;
    }

    inline uint64_t getSlotIdInPageForSlot(uint64_t slotId) const {
        return slotId % indexHeader.numSlotsPerPage;
    }

    uint8_t* getSlotFromPrimaryPages(uint64_t slotId) const;

    uint8_t* getOvfSlotFromOvfPages(uint64_t slotId) const;

    const uint8_t* getSlotInAPage(const uint8_t* page, uint32_t slotIdInPage) const {
        return page + (slotIdInPage * indexHeader.numBytesPerSlot);
    }

    inline uint8_t* getEntryInSlot(uint8_t* slot, uint32_t entryId) const {
        return slot + sizeof(SlotHeader) + (entryId * indexHeader.numBytesPerEntry);
    }

    // HashFunc
    inline static hash_t hashFuncForInt64(uint8_t* key) {
        hash_t hash;
        function::operation::Hash::operation(*(int64_t*)key, hash);
        return hash;
    }
    inline static hash_t hashFuncForString(uint8_t* key) {
        hash_t hash;
        function::operation::Hash::operation(string((char*)key), hash);
        return hash;
    }
    static hash_function_t initializeHashFunc(const DataType& dataType);

    // CopyFunc
    inline static void insertInt64KeyToEntryFunc(uint8_t* key, uint8_t* entry,
        InMemOverflowPages* overflowPages = nullptr, PageByteCursor* overflowCursor = nullptr) {
        memcpy(entry, key, Types::getDataTypeSize(INT64));
    }
    inline static void insertStringKeyToEntryFunc(uint8_t* key, uint8_t* entry,
        InMemOverflowPages* overflowPages, PageByteCursor* overflowCursor) {
        auto gfString =
            overflowPages->addString(reinterpret_cast<const char*>(key), *overflowCursor);
        memcpy(entry, &gfString, Types::getDataTypeSize(STRING));
    }
    static insert_function_t initializeInsertKeyToEntryFunc(const DataType& dataType);

    // This function checks if the prefix and length are the same.
    static bool isStringPrefixAndLenEquals(
        const uint8_t* keyToLookup, const gf_string_t* keyInEntry);
    // equalsFuncInWrite: used in the WRITE_MODE, when performing insertions.
    inline static bool equalsFuncInWriteModeForInt64(const uint8_t* keyToLookup,
        const uint8_t* keyInEntry, const InMemOverflowPages* overflowPages) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncInWriteModeForString(const uint8_t* keyToLookup,
        const uint8_t* keyInEntry, const InMemOverflowPages* overflowPages);
    static equals_in_write_function_t initializeEqualsFuncInWriteMode(const DataType& dataType);

    // equalsFuncInRead: used in the READ_MODE, when performing lookups.
    inline static bool equalsFuncInReadModeForInt64(
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowPages* overflowPages) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncInReadModeForString(
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowPages* overflowPages);
    static equals_in_read_function_t initializeEqualsFuncInReadMode(const DataType& dataType);

private:
    const string& fName;
    HashIndexMode indexMode;
    HashIndexHeader indexHeader;
    hash_function_t keyHashFunc;
    insert_function_t insertKeyToEntryFunc;
    equals_in_write_function_t equalsFuncInWrite;
    equals_in_read_function_t equalsFuncInRead;

    // used only when the hash index is instantiated in the write mode
    vector<unique_ptr<MemoryBlock>> primaryPages;
    vector<unique_ptr<MemoryBlock>> ovfPages;
    unique_ptr<InMemOverflowPages> inMemStringOvfPages;
    PageByteCursor stringOvfPageCursor;

    // used only when the hash index is instantiated in the read mode.
    unique_ptr<FileHandle> fileHandle;
    MemoryManager* memoryManager = nullptr;
    BufferManager* bufferManager = nullptr;
    unique_ptr<OverflowPages> stringOvfPages;
};

} // namespace storage
} // namespace graphflow
