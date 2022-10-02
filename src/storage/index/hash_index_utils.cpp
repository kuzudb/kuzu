#include "include/hash_index_utils.h"

namespace graphflow {
namespace storage {

in_mem_insert_function_t InMemHashIndexUtils::initializeInsertFunc(const DataTypeID& dataTypeID) {
    switch (dataTypeID) {
    case INT64:
        return insertFuncForInt64;
    case STRING:
        return insertFuncForString;
    default:
        throw StorageException(
            "Hash index insertion not defined for dataType other than INT64 and STRING.");
    }
}

bool InMemHashIndexUtils::equalsFuncForString(const uint8_t* keyToLookup, const uint8_t* keyInEntry,
    const InMemOverflowFile* inMemOverflowFile) {
    auto gfStringInEntry = (gf_string_t*)keyInEntry;
    // Checks if prefix and len matches first.
    if (!HashIndexUtils::isStringPrefixAndLenEquals(keyToLookup, gfStringInEntry)) {
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
        TypeUtils::decodeOverflowPtr(
            gfStringInEntry->overflowPtr, cursor.pageIdx, cursor.offsetInPage);
        return memcmp(keyToLookup,
                   inMemOverflowFile->getPage(cursor.pageIdx)->data + cursor.offsetInPage,
                   gfStringInEntry->len) == 0;
    }
}

in_mem_equals_function_t InMemHashIndexUtils::initializeEqualsFunc(const DataTypeID& dataTypeID) {
    switch (dataTypeID) {
    case INT64: {
        return equalsFuncForInt64;
    }
    case STRING: {
        return equalsFuncForString;
    }
    default: {
        throw CopyCSVException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
    }
}

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

bool HashIndexUtils::isStringPrefixAndLenEquals(
    const uint8_t* keyToLookup, const gf_string_t* keyInEntry) {
    auto prefixLen =
        min((uint64_t)keyInEntry->len, static_cast<uint64_t>(gf_string_t::PREFIX_LENGTH));
    return strlen(reinterpret_cast<const char*>(keyToLookup)) == keyInEntry->len &&
           memcmp(keyToLookup, keyInEntry->prefix, prefixLen) == 0;
}

bool HashIndexUtils::equalsFuncForString(
    const uint8_t* keyToLookup, const uint8_t* keyInEntry, DiskOverflowFile* diskOverflowFile) {
    auto keyInEntryString = (gf_string_t*)keyInEntry;
    if (isStringPrefixAndLenEquals(keyToLookup, keyInEntryString)) {
        auto entryKeyString = diskOverflowFile->readString(*keyInEntryString);
        return memcmp(keyToLookup, entryKeyString.c_str(), entryKeyString.length()) == 0;
    }
    return false;
}

equals_function_t HashIndexUtils::initializeEqualsFunc(const DataTypeID& dataTypeID) {
    switch (dataTypeID) {
    case INT64:
        return equalsFuncForInt64;
    case STRING:
        return equalsFuncForString;
    default:
        throw StorageException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
}

} // namespace storage
} // namespace graphflow
