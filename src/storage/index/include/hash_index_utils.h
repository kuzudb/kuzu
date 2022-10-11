#pragma once

#include <functional>

#include "src/function/hash/operations/include/hash_operations.h"
#include "src/storage/storage_structure/include/disk_overflow_file.h"
#include "src/storage/storage_structure/include/in_mem_file.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

using hash_function_t = std::function<hash_t(const uint8_t*)>;
using equals_function_t = std::function<bool(const uint8_t*, const uint8_t*, DiskOverflowFile*)>;

static const uint32_t NUM_BYTES_FOR_INT64_KEY = Types::getDataTypeSize(INT64);
static const uint32_t NUM_BYTES_FOR_STRING_KEY = Types::getDataTypeSize(STRING);
constexpr uint64_t INDEX_HEADER_PAGE_ID = 0;

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
        InMemOverflowFile* inMemOverflowFile = nullptr) {
        memcpy(entry, key, NUM_BYTES_FOR_INT64_KEY);
        memcpy(entry + NUM_BYTES_FOR_INT64_KEY, &offset, sizeof(node_offset_t));
    }
    inline static void insertFuncForString(const uint8_t* key, node_offset_t offset, uint8_t* entry,
        InMemOverflowFile* inMemOverflowFile) {
        auto gfString = inMemOverflowFile->appendString(reinterpret_cast<const char*>(key));
        memcpy(entry, &gfString, NUM_BYTES_FOR_STRING_KEY);
        memcpy(entry + NUM_BYTES_FOR_STRING_KEY, &offset, sizeof(node_offset_t));
    }
    inline static bool equalsFuncForInt64(const uint8_t* keyToLookup, const uint8_t* keyInEntry,
        const InMemOverflowFile* inMemOverflowFile = nullptr) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncForString(const uint8_t* keyToLookup, const uint8_t* keyInEntry,
        const InMemOverflowFile* inMemOverflowFile);
};

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

    // EqualsFunc
    static bool isStringPrefixAndLenEquals(
        const uint8_t* keyToLookup, const gf_string_t* keyInEntry);
    inline static bool equalsFuncForInt64(
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, DiskOverflowFile* ovfPages) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncForString(
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, DiskOverflowFile* diskOverflowFile);
    static equals_function_t initializeEqualsFunc(const DataTypeID& dataTypeID);
};
} // namespace storage
} // namespace graphflow
