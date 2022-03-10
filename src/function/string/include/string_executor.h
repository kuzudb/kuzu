#pragma once

#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {

struct StringOperationExecutor {

public:
    template<typename RESULT_TYPE, class OP>
    static void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        assert(left.dataType == right.dataType);
        switch (left.dataType) {
        case STRING: {
            BinaryOperationExecutor::execute<gf_string_t, gf_string_t, RESULT_TYPE, OP>(
                left, right, result);
        } break;
        // TODO: add unstructured support
        default:
            assert(false);
        }
    }

    template<class OP>
    static uint64_t select(ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        assert(left.dataType == right.dataType);
        switch (left.dataType) {
        case STRING: {
            return BinaryOperationExecutor::select<gf_string_t, gf_string_t, OP>(
                left, right, selectedPositions);
        } break;
        // TODO: add unstructured support
        default:
            assert(false);
        }
    }
};

} // namespace function
} // namespace graphflow
