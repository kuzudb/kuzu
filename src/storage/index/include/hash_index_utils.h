#pragma once

#include <functional>

#include "src/function/hash/operations/include/hash_operations.h"
#include "src/storage/storage_structure/include/overflow_file.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

using hash_function_t = std::function<hash_t(const uint8_t*)>;
using insert_function_t =
    std::function<void(const uint8_t*, node_offset_t, uint8_t*, OverflowFile*, PageByteCursor*)>;
using equals_function_t = std::function<bool(const uint8_t*, const uint8_t*, OverflowFile*)>;

static const uint32_t NUM_BYTES_FOR_INT64_KEY = Types::getDataTypeSize(INT64);
static const uint32_t NUM_BYTES_FOR_STRING_KEY = Types::getDataTypeSize(STRING);
constexpr uint64_t INDEX_HEADER_PAGE_ID = 0;

class SlotHeader {
public:
    SlotHeader() : numEntries{0}, deletionMask{0}, nextOvfSlotId{0} {}

    void reset() {
        numEntries = 0;
        deletionMask = 0;
        nextOvfSlotId = 0;
    }

    inline bool isEntryDeleted(uint32_t entryPos) const {
        return deletionMask & ((uint32_t)1 << entryPos);
    }
    inline void setEntryDeleted(uint32_t entryPos) { deletionMask |= ((uint32_t)1 << entryPos); }

public:
    uint8_t numEntries;
    uint32_t deletionMask;
    uint64_t nextOvfSlotId;
};

class HashIndexHeader {

public:
    explicit HashIndexHeader(DataTypeID keyDataType);
    HashIndexHeader(const HashIndexHeader& other) = default;

    inline void incrementLevel() {
        currentLevel++;
        levelHashMask = (1 << currentLevel) - 1;
        higherLevelHashMask = (1 << (currentLevel + 1)) - 1;
    }

public:
    uint32_t numBytesPerEntry{0};
    uint32_t numBytesPerSlot{0};
    uint32_t numSlotsPerPage{0};
    uint64_t numEntries{0};
    uint64_t currentLevel{0};
    uint64_t levelHashMask{0};
    uint64_t higherLevelHashMask{0};
    uint64_t nextSplitSlotId{0};
    DataTypeID keyDataTypeID;
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
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowFile* ovfPages) {
        return memcmp(keyToLookup, keyInEntry, sizeof(int64_t)) == 0;
    }
    static bool equalsFuncForString(
        const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowFile* ovfPages);
    static equals_function_t initializeEqualsFunc(const DataTypeID& dataTypeID);
};
} // namespace storage
} // namespace graphflow
