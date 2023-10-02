#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"
#include "function/string/functions/array_extract_function.h"

namespace kuzu {
namespace function {

struct ListExtract {
public:
    template<typename T>
    static inline void setValue(T& src, T& dest, common::ValueVector& resultValueVector) {
        dest = src;
    }

    // Note: this function takes in a 1-based position (The index of the first value in the list
    // is 1).
    template<typename T>
    static inline void operation(common::list_entry_t& listEntry, int64_t pos, T& result,
        common::ValueVector& listVector, common::ValueVector& posVector,
        common::ValueVector& resultVector) {
        if (pos == 0) {
            throw common::RuntimeException("List extract takes 1-based position.");
        }
        uint64_t upos = pos == -1 ? listEntry.size : pos;
        if (listEntry.size < upos) {
            throw common::RuntimeException("list_extract(list, index): index=" +
                                           common::TypeUtils::toString(pos) + " is out of range.");
        }
        if (listEntry.size == 0) {
            return; // TODO(Xiyang/Ziyi): we should fix when extracting last element of list.
        }
        auto listDataVector = common::ListVector::getDataVector(&listVector);
        auto listValues =
            common::ListVector::getListValuesWithOffset(&listVector, listEntry, upos - 1);
        resultVector.copyFromVectorData(
            reinterpret_cast<uint8_t*>(&result), listDataVector, listValues);
    }

    static inline void operation(
        common::ku_string_t& str, int64_t& idx, common::ku_string_t& result) {
        if (str.len < idx) {
            result.set("", 0);
        } else {
            ArrayExtract::operation(str, idx, result);
        }
    }
};

template<>
inline void ListExtract::setValue(
    common::ku_string_t& src, common::ku_string_t& dest, common::ValueVector& resultValueVector) {
    common::StringVector::addString(&resultValueVector, dest, src);
}

} // namespace function
} // namespace kuzu
