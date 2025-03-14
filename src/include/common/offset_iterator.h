#pragma once

#include "common/types/types.h"
#include <concepts>

namespace kuzu::common {
template<typename T, typename ReferenceType>
concept OffsetRangeLookup = requires(T t, common::offset_t offset) {
    { t.at(offset) } -> std::same_as<ReferenceType>;
};

/**
 * @brief Utility class that allows iteration using range-based for loops. Instantiations of this
 * class just need to provide a class that performs the lookup based on the current offset
 *
 * @tparam ReferenceType the return type of the lookup
 * @tparam Lookup the class that contains the lookup operation at(offset)
 */
template<typename ReferenceType, OffsetRangeLookup<ReferenceType> Lookup>
struct OffsetRange {
    struct Iterator {
        bool operator!=(const Iterator& o) const { return offset != o.offset; }
        ReferenceType operator*() const { return lookup.at(offset); }
        void operator++() { ++offset; }

        Lookup lookup;
        common::offset_t offset;
    };

    Iterator begin() const { return Iterator{lookup, startOffset}; }
    Iterator end() const { return Iterator{lookup, endOffset}; }

    Lookup lookup;
    common::offset_t startOffset;
    common::offset_t endOffset;
};
} // namespace kuzu::common
