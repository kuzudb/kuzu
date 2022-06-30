#pragma once

#include <bitset>
#include <climits>
#include <iostream>

#include "hash_index_utils.h"
#include "in_mem_hash_index.h"

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/storage/storage_structure/include/overflow_file.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class HashIndex : public BaseHashIndex {

    static constexpr bool DELETE_FLAG = true;
    static constexpr bool LOOKUP_FLAG = false;

public:
    HashIndex(const StorageStructureIDAndFName& storageStructureIDAndFName,
        const DataType& keyDataType, BufferManager& bufferManager, bool isInMemory);

    ~HashIndex();

public:
    inline bool lookup(int64_t key, node_offset_t& result) {
        return lookupOrDeleteInternal<LOOKUP_FLAG>(reinterpret_cast<uint8_t*>(&key), &result);
    }
    inline bool lookup(const char* key, node_offset_t& result) {
        return lookupOrDeleteInternal<LOOKUP_FLAG>(reinterpret_cast<const uint8_t*>(key), &result);
    }
    inline bool deleteKey(int64_t key) {
        return lookupOrDeleteInternal<DELETE_FLAG>(reinterpret_cast<uint8_t*>(&key));
    }
    inline bool deleteKey(const char* key) {
        return lookupOrDeleteInternal<DELETE_FLAG>(reinterpret_cast<const uint8_t*>(key));
    }

    // Note: This function is NOT thread-safe.
    void flush() override;

private:
    void initializeFileAndHeader(const DataType& keyDataType);
    void readPageMapper(vector<page_idx_t>& mapper, page_idx_t mapperFirstPageIdx);

    uint8_t* pinSlot(const SlotInfo& slotInfo);
    void unpinSlot(const SlotInfo& slotInfo);

    template<bool IS_DELETE>
    bool lookupOrDeleteInternal(const uint8_t* key, node_offset_t* result = nullptr);
    template<bool IS_DELETE>
    bool lookupOrDeleteInSlot(
        uint8_t* slot, const uint8_t* key, node_offset_t* result = nullptr) const;

private:
    StorageStructureIDAndFName storageStructureIDAndFName;
    bool isInMemory;
    unique_ptr<FileHandle> fh;
    BufferManager& bm;
    equals_function_t keyEqualsFunc;
    unique_ptr<OverflowFile> overflowFile;
};

} // namespace storage
} // namespace graphflow
