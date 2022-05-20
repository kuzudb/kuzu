#include "src/storage/include/index/hash_index_utils.h"

namespace graphflow {
namespace storage {

hash_function_t HashIndexUtils::initializeHashFunc(const DataTypeID& dataTypeID) {
    switch (dataTypeID) {
    case INT64:
        return hashFuncForInt64;
    case STRING:
        return hashFuncForString;
    default:
        throw StorageException("Type " + Types::dataTypeToString(dataTypeID) + " not supported.");
    }
}

insert_function_t HashIndexUtils::initializeInsertKeyToEntryFunc(const DataTypeID& dataTypeID) {
    switch (dataTypeID) {
    case INT64:
        return insertInt64KeyToEntryFunc;
    case STRING:
        return insertStringKeyToEntryFunc;
    default:
        throw StorageException(
            "Hash index insertion not defined for dataType other than INT64 and STRING.");
    }
}

bool HashIndexUtils::isStringPrefixAndLenEquals(
    const uint8_t* keyToLookup, const gf_string_t* keyInEntry) {
    auto prefixLen =
        min((uint64_t)keyInEntry->len, static_cast<uint64_t>(gf_string_t::PREFIX_LENGTH));
    if (strlen(reinterpret_cast<const char*>(keyToLookup)) == keyInEntry->len &&
        memcmp(keyToLookup, keyInEntry->prefix, prefixLen) == 0) {
        return true;
    }
    return false;
}

bool HashIndexUtils::equalsFuncInWriteModeForString(const uint8_t* keyToLookup,
    const uint8_t* keyInEntry, const InMemOverflowPages* overflowPages) {
    auto gfStringInEntry = (gf_string_t*)keyInEntry;
    // Checks if prefix and len matches first.
    if (!isStringPrefixAndLenEquals(keyToLookup, gfStringInEntry)) {
        return false;
    }
    if (gfStringInEntry->len <= gf_string_t::PREFIX_LENGTH) {
        // For strings shorter than PREFIX_LENGTH, the result must be true.
        return true;
    } else if (gfStringInEntry->len <= gf_string_t::SHORT_STR_LENGTH) {
        // For short strings, whose lengths are larger than PREFIX_LENGTH, check if their actual
        // values are equal.
        return memcmp(keyToLookup, gfStringInEntry->prefix, gfStringInEntry->len) == 0;
    } else {
        // For long strings, read overflow values and check if they are true.
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(gfStringInEntry->overflowPtr, cursor.idx, cursor.offset);
        return memcmp(keyToLookup, overflowPages->pages[cursor.idx]->data + cursor.offset,
                   gfStringInEntry->len) == 0;
    }
}

equals_in_write_function_t HashIndexUtils::initializeEqualsFuncInWriteMode(
    const DataTypeID& dataTypeID) {
    switch (dataTypeID) {
    case INT64:
        return equalsFuncInWriteModeForInt64;
    case STRING:
        return equalsFuncInWriteModeForString;
    default:
        throw StorageException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
}

bool HashIndexUtils::equalsFuncInReadModeForString(
    const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowPages* overflowPages) {
    auto keyInEntryString = (gf_string_t*)keyInEntry;
    if (isStringPrefixAndLenEquals(keyToLookup, keyInEntryString)) {
        auto entryKeyString = overflowPages->readString(*keyInEntryString);
        return memcmp(keyToLookup, entryKeyString.c_str(), entryKeyString.length()) == 0;
    }
    return false;
}

equals_in_read_function_t HashIndexUtils::initializeEqualsFuncInReadMode(
    const DataTypeID& dataTypeID) {
    switch (dataTypeID) {
    case INT64:
        return equalsFuncInReadModeForInt64;
    case STRING:
        return equalsFuncInReadModeForString;
    default:
        throw StorageException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
}

} // namespace storage
} // namespace graphflow
