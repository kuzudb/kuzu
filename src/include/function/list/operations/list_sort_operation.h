#pragma once

#include "base_list_sort_operation.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

template<typename T>
struct ListSort : BaseListSortOperation {
    static inline void operation(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        sortValues<T>(
            input, result, inputVector, resultVector, true /* ascOrder */, true /* nullFirst */);
    }

    static inline void operation(common::list_entry_t& input, common::ku_string_t& sortOrder,
        common::list_entry_t& result, common::ValueVector& inputVector,
        common::ValueVector& valueVector, common::ValueVector& resultVector) {
        sortValues<T>(input, result, inputVector, resultVector, isAscOrder(sortOrder.getAsString()),
            true /* nullFirst */);
    }

    static inline void operation(common::list_entry_t& input, common::ku_string_t& sortOrder,
        common::ku_string_t& nullOrder, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        sortValues<T>(input, result, inputVector, resultVector, isAscOrder(sortOrder.getAsString()),
            isNullFirst(nullOrder.getAsString()));
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
