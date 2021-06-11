#pragma once

#include <bitset>

#include "src/common/include/configs.h"
#include "src/common/include/memory_manager.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_utils.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace storage {

const string OVERFLOW_FILE_NAME = "overflow.index";
// The num of pages inside each page group is the same as the num of bits of a page, due to that in
// the bitset page, we use one bit per page to decide if the page in the group is allocated or not.
constexpr uint64_t NUM_PAGES_PER_PAGE_GROUP = PAGE_SIZE * 8;

class OverflowPagesManager {
public:
    OverflowPagesManager(const string& indexPath, MemoryManager& memoryManager);

    uint8_t* getMemoryBlock(uint64_t blockId);
    uint64_t allocateFreeOverflowPage();
    void flush();

    // Each overflow page (except for the bitset page per page group) has a page header, which
    // contains a bitset to indicate if each slot in the page is used or not.
    static void serOverflowPageHeader(uint8_t* block, vector<bool>& bitset);
    static vector<bool> deSerOverflowPageHeader(const uint8_t* block, uint64_t numBits);

    unique_ptr<FileHandle> fileHandle;

private:
    void serOverflowPagesAllocationBitsets();
    void deSerOverflowPagesAllocationBitsets();

private:
    MemoryManager& memoryManager;

    unordered_map<uint64_t, unique_ptr<MemoryBlock>> memoryBlocks;
    // TRUE: page already allocated for usage; FALSE: page is available for allocation
    vector<bitset<NUM_PAGES_PER_PAGE_GROUP>> overflowPagesAllocationBitsets;
};
} // namespace storage
} // namespace graphflow
