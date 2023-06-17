#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

struct BaseListSortOperation {
public:
    static inline bool isAscOrder(const std::string& sortOrder) {
        std::string upperSortOrder = toUpperCase(sortOrder);
        if (upperSortOrder == "ASC") {
            return true;
        } else if (upperSortOrder == "DESC") {
            return false;
        } else {
            throw common::RuntimeException("Invalid sortOrder");
        }
    }

    static inline bool isNullFirst(const std::string& nullOrder) {
        std::string upperNullOrder = toUpperCase(nullOrder);
        if (upperNullOrder == "NULLS FIRST") {
            return true;
        } else if (upperNullOrder == "NULLS LAST") {
            return false;
        } else {
            throw common::RuntimeException("Invalid nullOrder");
        }
    }

    static inline std::string toUpperCase(const std::string& str) {
        std::string upperStr = str;
        std::transform(upperStr.begin(), upperStr.end(), upperStr.begin(), ::toupper);
        return upperStr;
    }

    template<typename T>
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
            resultDataVector->copyFromVectorData(resultValues, inputDataVector, inputValues);
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
