#pragma once

#include <functional>

#include "src/loader/in_mem_storage_structure/include/in_mem_file.h"
#include "src/storage/index/include/hash_index_utils.h"

namespace graphflow {
namespace loader {

using in_mem_insert_function_t =
    std::function<void(const uint8_t*, node_offset_t, uint8_t*, InMemOverflowFile*)>;
using in_mem_equals_function_t =
    std::function<bool(const uint8_t*, const uint8_t*, const InMemOverflowFile*)>;

class InMemHashIndexUtils {
public:
    static in_mem_equals_function_t initializeEqualsFunc(const DataTypeID& dataTypeID);
    static in_mem_insert_function_t initializeInsertFunc(const DataTypeID& dataTypeID);

private:
    // InsertFunc
    inline static void insertFuncForInt64(const uint8_t* key, node_offset_t offset, uint8_t* entry,
        InMemOverflowFile* overflowFile = nullptr) {
        memcpy(entry, key, NUM_BYTES_FOR_INT64_KEY);
        memcpy(entry + NUM_BYTES_FOR_INT64_KEY, &offset, sizeof(node_offset_t));
    }
    inline static void insertFuncForString(
        const uint8_t* key, node_offset_t offset, uint8_t* entry, InMemOverflowFile* overflowFile) {
        auto gfString = overflowFile->appendString(reinterpret_cast<const char*>(key));
        memcpy(entry, &gfString, NUM_BYTES_FOR_STRING_KEY);
        memcpy(entry + NUM_BYTES_FOR_STRING_KEY, &offset, sizeof(node_offset_t));
    }
    inline static bool equalsFuncForInt64(const uint8_t* keyToLookup, const uint8_t* keyInEntry,
        const InMemOverflowFile* overflowFile = nullptr) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncForString(const uint8_t* keyToLookup, const uint8_t* keyInEntry,
        const InMemOverflowFile* overflowFile);
};

} // namespace loader
} // namespace graphflow
