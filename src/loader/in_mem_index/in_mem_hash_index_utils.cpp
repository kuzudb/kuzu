#include "src/loader/in_mem_index/include/in_mem_hash_index_utils.h"

#include "src/common/include/type_utils.h"
#include "src/storage/index/include/hash_index_utils.h"

namespace graphflow {
namespace loader {

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

bool InMemHashIndexUtils::equalsFuncForString(
    const uint8_t* keyToLookup, const uint8_t* keyInEntry, const InMemOverflowFile* overflowFile) {
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
                   overflowFile->getPage(cursor.pageIdx)->data + cursor.offsetInPage,
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
        throw LoaderException(
            "Hash index equals is not supported for dataType other than INT64 and STRING.");
    }
    }
}

} // namespace loader
} // namespace graphflow
