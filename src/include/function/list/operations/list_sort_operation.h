#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

template<typename T>
struct ListSort {
    static inline void operation(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        sortValues(
            input, result, inputVector, resultVector, true /* ascOrder */, true /* nullFirst */);
    }

    static inline void operation(common::list_entry_t& input, common::ku_string_t& sortOrder,
        common::list_entry_t& result, common::ValueVector& inputVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector) {
        sortValues(input, result, inputVector, resultVector, isAscOrder(sortOrder.getAsString()),
            true /* nullFirst */);
    }

    static inline void operation(common::list_entry_t& input, common::ku_string_t& sortOrder,
        common::ku_string_t& nullOrder, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        sortValues(input, result, inputVector, resultVector, isAscOrder(sortOrder.getAsString()),
            isNullFirst(nullOrder.getAsString()));
    }

    static inline bool isAscOrder(const std::string& sortOrder) {
        if (sortOrder == "ASC") {
            return true;
        } else if (sortOrder == "DESC") {
            return false;
        } else {
            throw common::RuntimeException("Invalid sortOrder");
        }
    }

    static inline bool isNullFirst(const std::string& nullOrder) {
        if (nullOrder == "NULLS FIRST") {
            return true;
        } else if (nullOrder == "NULLS LAST") {
            return false;
        } else {
            throw common::RuntimeException("Invalid nullOrder");
        }
    }

    static void sortValues(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector, bool ascOrder,
        bool nullFirst) {
        // TODO(Ziyi) - Replace this sort implementation with radix_sort implementation:
        //  https://github.com/kuzudb/kuzu/issues/1536.
        auto inputValues = common::ListVector::getListValues(&inputVector, input);
        auto inputDataVector = common::ListVector::getDataVector(&inputVector);

        // Calculate null count.
        auto nullCount = 0;
        for (auto i = 0; i < input.size; i++) {
            if (inputDataVector->isNull(input.offset + i)) {
                nullCount += 1;
            }
        }

        result = common::ListVector::addList(&resultVector, input.size);
        auto resultValues = common::ListVector::getListValues(&resultVector, result);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto numBytesPerValue = resultDataVector->getNumBytesPerValue();

        // Add nulls first.
        if (nullFirst) {
            setVectorRangeToNull(*resultDataVector, result.offset, 0, nullCount);
            resultValues += numBytesPerValue * nullCount;
        }

        // Add actual data.
        for (auto i = 0; i < input.size; i++) {
            if (inputDataVector->isNull(input.offset + i)) {
                inputValues += numBytesPerValue;
                continue;
            }
            common::ValueVectorUtils::copyValue(
                resultValues, *resultDataVector, inputValues, *inputDataVector);
            resultValues += numBytesPerValue;
            inputValues += numBytesPerValue;
        }

        // Add nulls in the end.
        if (!nullFirst) {
            setVectorRangeToNull(
                *resultDataVector, result.offset, input.size - nullCount, input.size);
            resultValues += numBytesPerValue * nullCount;
        }

        // Determine the starting and ending position of the data to be sorted.
        auto sortStart = nullCount;
        auto sortEnd = input.size;
        if (!nullFirst) {
            sortStart = 0;
            sortEnd = input.size - nullCount;
        }

        // Sort the data based on order.
        auto sortingValues =
            reinterpret_cast<T*>(common::ListVector::getListValues(&resultVector, result));
        if (ascOrder) {
            std::sort(sortingValues + sortStart, sortingValues + sortEnd, std::less{});
        } else {
            std::sort(sortingValues + sortStart, sortingValues + sortEnd, std::greater{});
        }
    }

    static void setVectorRangeToNull(
        common::ValueVector& vector, uint64_t offset, uint64_t startPos, uint64_t endPos) {
        for (auto i = startPos; i < endPos; i++) {
            vector.setNull(offset + i, true);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
