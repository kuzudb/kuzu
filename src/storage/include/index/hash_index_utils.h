#pragma once

#include <functional>

#include "src/function/hash/operations/include/hash_operations.h"
#include "src/loader/include/in_mem_structure/in_mem_file.h"
#include "src/storage/include/storage_structure/overflow_pages.h"

using namespace graphflow::common;
using namespace graphflow::loader;

namespace graphflow {
namespace storage {

using hash_function_t = std::function<hash_t(const uint8_t*)>;
using insert_function_t =
    std::function<void(const uint8_t*, uint8_t*, InMemOverflowFile*, PageByteCursor*)>;
using equals_in_write_function_t =
    std::function<bool(const uint8_t*, const uint8_t*, const InMemOverflowFile*)>;
using equals_in_read_function_t =
    std::function<bool(const uint8_t*, const uint8_t*, OverflowPages*)>;

class HashIndexUtils {

public:
    // HashFunc
    inline static hash_t hashFuncForInt64(const uint8_t* key) {
        hash_t hash;
        function::operation::Hash::operation(*(int64_t*)key, hash);
        return hash;
    }
    inline static hash_t hashFuncForString(const uint8_t* key) {
        hash_t hash;
        function::operation::Hash::operation(string((char*)key), hash);
        return hash;
    }
    static hash_function_t initializeHashFunc(const DataTypeID& dataTypeID);
    // InsertFunc
    inline static void insertInt64KeyToEntryFunc(const uint8_t* key, uint8_t* entry,
        InMemOverflowFile* overflowPages = nullptr, PageByteCursor* overflowCursor = nullptr) {
        memcpy(entry, key, Types::getDataTypeSize(INT64));
    }
    inline static void insertStringKeyToEntryFunc(const uint8_t* key, uint8_t* entry,
        InMemOverflowFile* overflowPages, PageByteCursor* overflowCursor) {
        auto gfString =
            overflowPages->addString(reinterpret_cast<const char*>(key), *overflowCursor);
        memcpy(entry, &gfString, Types::getDataTypeSize(STRING));
    }
    static insert_function_t initializeInsertKeyToEntryFunc(const DataTypeID& dataTypeID);
    // This function checks if the prefix and length are the same.
    static bool isStringPrefixAndLenEquals(
        const uint8_t* keyToLookup, const gf_string_t* keyInEntry);
    // equalsFuncInWrite: used in the WRITE_MODE, when performing insertions.
    inline static bool equalsFuncInWriteModeForInt64(const uint8_t* keyToLookup,
        const uint8_t* keyInEntry, const InMemOverflowFile* overflowPages) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncInWriteModeForString(const uint8_t* keyToLookup,
        const uint8_t* keyInEntry, const InMemOverflowFile* overflowPages);
    static equals_in_write_function_t initializeEqualsFuncInWriteMode(const DataTypeID& dataTypeID);
    // equalsFuncInRead: used in the READ_MODE, when performing lookups.
    inline static bool equalsFuncInReadModeForInt64(
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowPages* ovfPages) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncInReadModeForString(
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowPages* ovfPages);
    static equals_in_read_function_t initializeEqualsFuncInReadMode(const DataTypeID& dataTypeID);
};
} // namespace storage
} // namespace graphflow
