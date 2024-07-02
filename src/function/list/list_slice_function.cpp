
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"
#include "function/string/functions/substr_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static void normalizeIndices(int64_t& startIdx, int64_t& endIdx, uint64_t size) {
    if (startIdx < 0) {
        startIdx = size + startIdx + 1;
    }
    if (endIdx <= 0) {
        endIdx = size + endIdx + 1;
    }

    if (startIdx <= 0) {
        startIdx = 1;
    }

    if ((uint64_t)endIdx > size) {
        endIdx = size + 1;
    }

    if (startIdx > endIdx) {
        endIdx = startIdx;
    }
}

struct ListSlice {
    // Note: this function takes in a 1-based begin/end index (The index of the first value in the
    // listEntry is 1).
    static void operation(common::list_entry_t& listEntry, int64_t& begin, int64_t& end,
        common::list_entry_t& result, common::ValueVector& listVector,
        common::ValueVector& resultVector) {
        auto startIdx = begin;
        auto endIdx = end;
        normalizeIndices(startIdx, endIdx, listEntry.size);
        result = common::ListVector::addList(&resultVector, endIdx - startIdx);
        auto srcDataVector = common::ListVector::getDataVector(&listVector);
        auto srcPos = listEntry.offset + startIdx - 1;
        auto dstDataVector = common::ListVector::getDataVector(&resultVector);
        auto dstPos = result.offset;
        for (; startIdx < endIdx; startIdx++) {
            dstDataVector->copyFromVectorData(dstPos++, srcDataVector, srcPos++);
        }
    }

    static void operation(common::ku_string_t& str, int64_t& begin, int64_t& end,
        common::ku_string_t& result, common::ValueVector& /*listValueVector*/,
        common::ValueVector& resultValueVector) {
        auto startIdx = begin;
        auto endIdx = end;
        normalizeIndices(startIdx, endIdx, str.len);
        SubStr::operation(str, startIdx, endIdx - startIdx + 1, result, resultValueVector);
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    KU_ASSERT(arguments.size() == 3);
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType().copy());
    paramTypes.push_back(LogicalType(function->parameterTypeIDs[1]));
    paramTypes.push_back(LogicalType(function->parameterTypeIDs[2]));
    return std::make_unique<FunctionBindData>(std::move(paramTypes),
        arguments[0]->getDataType().copy());
}

function_set ListSliceFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::LIST,
        ScalarFunction::TernaryExecListStructFunction<list_entry_t, int64_t, int64_t, list_entry_t,
            ListSlice>,
        nullptr /* selectFunc */, bindFunc));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
            LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryExecListStructFunction<ku_string_t, int64_t, int64_t, ku_string_t,
            ListSlice>,
        nullptr /* selectFunc */, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
