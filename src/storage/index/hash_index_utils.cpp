#include "include/hash_index_utils.h"

namespace graphflow {
namespace storage {

HashIndexHeader::HashIndexHeader(DataTypeID keyDataTypeID) : keyDataTypeID{keyDataTypeID} {
    numBytesPerEntry = Types::getDataTypeSize(this->keyDataTypeID) + sizeof(node_offset_t);
    numBytesPerSlot =
        (numBytesPerEntry << HashIndexConfig::SLOT_CAPACITY_LOG_2) + sizeof(SlotHeader);
    assert(numBytesPerSlot < DEFAULT_PAGE_SIZE);
    numSlotsPerPage = DEFAULT_PAGE_SIZE / numBytesPerSlot;
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
    const uint8_t* keyToLookup, const uint8_t* keyInEntry, OverflowFile* overflowPages) {
    auto keyInEntryString = (gf_string_t*)keyInEntry;
    if (isStringPrefixAndLenEquals(keyToLookup, keyInEntryString)) {
        auto entryKeyString = overflowPages->readString(*keyInEntryString);
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
