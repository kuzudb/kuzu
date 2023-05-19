#include "storage/index/hash_index_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

in_mem_insert_function_t InMemHashIndexUtils::initializeInsertFunc(LogicalTypeID dataTypeID) {
    switch (dataTypeID) {
    case LogicalTypeID::INT64: {
        return insertFuncForInt64;
    }
    case LogicalTypeID::STRING: {
        return insertFuncForString;
    }
    default: {
        throw StorageException(
            "Hash index insertion not defined for dataType other than INT64 and STRING.");
    }
    }
}

bool InMemHashIndexUtils::equalsFuncForString(const uint8_t* keyToLookup, const uint8_t* keyInEntry,
    const InMemOverflowFile* inMemOverflowFile) {
    auto kuStringInEntry = (ku_string_t*)keyInEntry;
    // Checks if prefix and len matches first.
    if (!HashIndexUtils::isStringPrefixAndLenEquals(keyToLookup, kuStringInEntry)) {
        return false;
    }
    if (kuStringInEntry->len <= ku_string_t::PREFIX_LENGTH) {
        // For strings shorter than PREFIX_LENGTH, the result must be true.
        return true;
    } else if (kuStringInEntry->len <= ku_string_t::SHORT_STR_LENGTH) {
        // For short strings, whose lengths are larger than PREFIX_LENGTH, check if their actual
        // values are equal.
        return memcmp(keyToLookup, kuStringInEntry->prefix, kuStringInEntry->len) == 0;
    } else {
        // For long strings, read overflow values and check if they are true.
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(
            kuStringInEntry->overflowPtr, cursor.pageIdx, cursor.offsetInPage);
        return memcmp(keyToLookup,
                   inMemOverflowFile->getPage(cursor.pageIdx)->data + cursor.offsetInPage,
                   kuStringInEntry->len) == 0;
    }
}

in_mem_equals_function_t InMemHashIndexUtils::initializeEqualsFunc(LogicalTypeID dataTypeID) {
    switch (dataTypeID) {
    case LogicalTypeID::INT64: {
        return equalsFuncForInt64;
    }
    case LogicalTypeID::STRING: {
        return equalsFuncForString;
    }
    default: {
        throw CopyException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
    }
}

insert_function_t HashIndexUtils::initializeInsertFunc(LogicalTypeID dataTypeID) {
    switch (dataTypeID) {
    case LogicalTypeID::INT64: {
        return insertFuncForInt64;
    }
    case LogicalTypeID::STRING: {
        return insertFuncForString;
    }
    default: {
        throw StorageException(
            "Type " + LogicalTypeUtils::dataTypeToString(dataTypeID) + " not supported.");
    }
    }
}

hash_function_t HashIndexUtils::initializeHashFunc(LogicalTypeID dataTypeID) {
    switch (dataTypeID) {
    case LogicalTypeID::INT64: {
        return hashFuncForInt64;
    }
    case LogicalTypeID::STRING: {
        return hashFuncForString;
    }
    default: {
        throw StorageException(
            "Type " + LogicalTypeUtils::dataTypeToString(dataTypeID) + " not supported.");
    }
    }
}

bool HashIndexUtils::isStringPrefixAndLenEquals(
    const uint8_t* keyToLookup, const ku_string_t* keyInEntry) {
    auto prefixLen =
        std::min((uint64_t)keyInEntry->len, static_cast<uint64_t>(ku_string_t::PREFIX_LENGTH));
    return strlen(reinterpret_cast<const char*>(keyToLookup)) == keyInEntry->len &&
           memcmp(keyToLookup, keyInEntry->prefix, prefixLen) == 0;
}

bool HashIndexUtils::equalsFuncForString(TransactionType trxType, const uint8_t* keyToLookup,
    const uint8_t* keyInEntry, DiskOverflowFile* diskOverflowFile) {
    auto keyInEntryString = (ku_string_t*)keyInEntry;
    if (isStringPrefixAndLenEquals(keyToLookup, keyInEntryString)) {
        auto entryKeyString = diskOverflowFile->readString(trxType, *keyInEntryString);
        return memcmp(keyToLookup, entryKeyString.c_str(), entryKeyString.length()) == 0;
    }
    return false;
}

equals_function_t HashIndexUtils::initializeEqualsFunc(LogicalTypeID dataTypeID) {
    switch (dataTypeID) {
    case LogicalTypeID::INT64: {
        return equalsFuncForInt64;
    }
    case LogicalTypeID::STRING: {
        return equalsFuncForString;
    }
    default: {
        throw StorageException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
    }
}

} // namespace storage
} // namespace kuzu
