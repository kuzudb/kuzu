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

class HashIndexReader {

public:
    HashIndexReader(const string& fName, BufferManager& bufferManager, bool isInMemory);

public:
    bool lookup(uint8_t* key, node_offset_t& result, BufferManagerMetrics& metrics);

private:
    bool lookupInSlot(const uint8_t* slot, uint8_t* key, node_offset_t& result,
        BufferManagerMetrics& metrics) const;

private:
    const string& fName;
    HashIndexHeader indexHeader;
    shared_ptr<spdlog::logger> logger;

    unique_ptr<FileHandle> fileHandle;
    BufferManager* bufferManager{};
    unique_ptr<StringOverflowPages> stringOvfPages;
};

} // namespace storage
} // namespace graphflow
