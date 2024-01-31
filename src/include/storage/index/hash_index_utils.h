#pragma once

#include <functional>

#include "common/types/ku_string.h"
#include "function/hash/hash_functions.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

// NOLINTBEGIN(cert-err58-cpp): This is the best way to get the datatype size because it avoids
// refactoring.
static const uint32_t NUM_BYTES_FOR_INT64_KEY =
    storage::StorageUtils::getDataTypeSize(common::LogicalType{common::LogicalTypeID::INT64});
static const uint32_t NUM_BYTES_FOR_STRING_KEY =
    storage::StorageUtils::getDataTypeSize(common::LogicalType{common::LogicalTypeID::STRING});
// NOLINTEND(cert-err58-cpp)

const uint64_t NUM_HASH_INDEXES_LOG2 = 8;
const uint64_t NUM_HASH_INDEXES = 1 << NUM_HASH_INDEXES_LOG2;

template<typename T>
concept HashablePrimitive = ((std::integral<T> && !std::is_same_v<T, bool>) ||
                             std::floating_point<T> || std::is_same_v<T, common::int128_t>);

template<typename T>
concept Hashable = ((std::integral<T> && !std::is_same_v<T, bool>) || std::floating_point<T> ||
                    std::is_same_v<T, common::int128_t> || std::is_same_v<T, common::ku_string_t>);

inline uint64_t getHashIndexPosition(HashablePrimitive auto key) {
    common::hash_t hash;
    function::Hash::operation(key, hash);
    return (hash >> (64 - NUM_HASH_INDEXES_LOG2)) & (NUM_HASH_INDEXES - 1);
}
inline uint64_t getHashIndexPosition(std::string_view key) {
    return (std::hash<std::string_view>()(key) >> (64 - NUM_HASH_INDEXES_LOG2)) &
           (NUM_HASH_INDEXES - 1);
}

class HashIndexUtils {

public:
    inline static bool areStringPrefixAndLenEqual(
        std::string_view keyToLookup, const common::ku_string_t& keyInEntry) {
        auto prefixLen = std::min(
            (uint64_t)keyInEntry.len, static_cast<uint64_t>(common::ku_string_t::PREFIX_LENGTH));
        return keyToLookup.length() == keyInEntry.len &&
               memcmp(keyToLookup.data(), keyInEntry.prefix, prefixLen) == 0;
    }
};
} // namespace storage
} // namespace kuzu
