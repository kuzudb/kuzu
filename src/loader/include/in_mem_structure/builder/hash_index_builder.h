#pragma once

#include <bitset>
#include <climits>

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"
#include "src/loader/include/in_mem_structure/in_mem_pages.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/string_overflow_pages.h"
#include "src/storage/include/index/utils.h"

using namespace std;
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
 *       -----------------------
 *  0    | PAGE 0 (HEADER)     |
 *       -----------------------
 *  1    | PAGE 0 (HEADER)     |  <-|
 *       -----------------------    |
 *  2    | PAGE 0 (HEADER)     |    |
 *       -----------------------    |  PRIMARY PAGES
 *               ...                |
 *       -----------------------    |
 *  n    | PAGE 0 (HEADER)     |  <-|
 *       -----------------------
 *  n+1  | PAGE 0 (HEADER)     |  <-|
 *       -----------------------    |
 *  n+2  | PAGE 0 (HEADER)     |    |
 *       -----------------------    |  OVERFLOW PAGES
 *               ...                |
 *       -----------------------    |
 *  n+m  | PAGE 0 (HEADER)     |  <-|
 *       -----------------------
 *
 *  Information about the number of primary pages, primary slots, overflow pages and overflow
 *  slots are stored in the HashIndexHeader.
 *
 *  Mode Of Operations:
 *
 *  WRITE MODE: The hashIndex is initially opened in the write mode in which only the insertions
 *  are supported. In here, all the page allocations are happen in memory which needs to be saved
 *  to disk to persist. Also for performance reasons and based on our use case, we support
 *  `bulkReserve` operation to fix the hashIndex capacity in the beginning before any insertions
 *  are made. This is done by means of calculating the number of primary slots and pages are needed
 *  and pre-allocating them.  In lieu of this, we do not support changing the capacity dynamically
 *  by rehashing and splitting of slots. With this scenario, the hashIndex can still insert entries
 *  more than its capacity but these entries will land in chained overflow slots leading to
 *  degraded performance in insertions as well as in look ups.
 *
 *  READ MODE: The hashIndex can be opened in the read mode by supplying it with the name of already
 *  existing file that was previously flushed in the write mode. Lookups happen by reading
 *  arbitrary number of required pages to reach the required slot and iterating it to find the
 *  required value. In this mode, all pages are not kept in memory but rather are made accessible
 *  by our Buffer manager.
 *
 *  */
class HashIndexBuilder {

public:
    explicit HashIndexBuilder(const string& fName, DataType keyDataType);

public:
    // Reserves space for at least the specified number of elements.
    void bulkReserve(uint32_t numEntries);

    bool insert(uint8_t* key, node_offset_t value);

    void saveToDisk();

private:
    bool notExistsInSlot(uint8_t* slot, uint8_t* key);
    void putNewEntryInSlotAndUpdateHeader(uint8_t* slot, uint8_t* key, node_offset_t value);
    uint64_t reserveOvfSlot();
    void insertKey(uint8_t* key, uint8_t* entry);

    uint8_t* getSlotFromPrimaryPages(uint64_t slotId) const;

    uint8_t* getOvfSlotFromOvfPages(uint64_t slotId) const;

private:
    const string& fName;
    HashIndexHeader indexHeader;
    shared_ptr<spdlog::logger> logger;

    vector<unique_ptr<uint8_t[]>> primaryPages{};
    vector<unique_ptr<uint8_t[]>> ovfPages{};
    unique_ptr<InMemStringOverflowPages> inMemStringOvfPages;
    PageByteCursor stringOvfPageCursor;
};

} // namespace storage
} // namespace graphflow
