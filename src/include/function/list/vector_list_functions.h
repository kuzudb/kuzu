#pragma once

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct ListCreationFunction {
    static constexpr const char* name = "LIST_CREATION";

    static function_set getFunctionSet();
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr);
};

struct ListRangeFunction {
    static constexpr const char* name = "RANGE";

    static function_set getFunctionSet();
};

struct SizeFunction {
    static constexpr const char* name = "SIZE";

    static function_set getFunctionSet();
};

struct CardinalityFunction {
    using alias = SizeFunction;

    static constexpr const char* name = "CARDINALITY";
};

struct ListExtractFunction {
    static constexpr const char* name = "LIST_EXTRACT";

    static function_set getFunctionSet();
};

struct ListElementFunction {
    using alias = ListExtractFunction;

    static constexpr const char* name = "LIST_ELEMENT";
};

struct ListConcatFunction {
    static constexpr const char* name = "LIST_CONCAT";

    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
        Function* function);
};

struct ListCatFunction {
    using alias = ListConcatFunction;

    static constexpr const char* name = "LIST_CAT";
};

struct ListAppendFunction {
    static constexpr const char* name = "LIST_APPEND";

    static function_set getFunctionSet();
};

struct ListPrependFunction {
    static constexpr const char* name = "LIST_PREPEND";

    static function_set getFunctionSet();
};

struct ListPositionFunction {
    static constexpr const char* name = "LIST_POSITION";

    static function_set getFunctionSet();
};

struct ListIndexOfFunction {
    using alias = ListPositionFunction;

    static constexpr const char* name = "LIST_INDEXOF";
};

struct ListContainsFunction {
    static constexpr const char* name = "LIST_CONTAINS";

    static function_set getFunctionSet();
};

struct ListHasFunction {
    using alias = ListContainsFunction;

    static constexpr const char* name = "LIST_HAS";
};

struct ListSliceFunction {
    static constexpr const char* name = "LIST_SLICE";

    static function_set getFunctionSet();
};

struct ListSortFunction {
    static constexpr const char* name = "LIST_SORT";

    static function_set getFunctionSet();
};

struct ListReverseSortFunction {
    static constexpr const char* name = "LIST_REVERSE_SORT";

    static function_set getFunctionSet();
};

struct ListSumFunction {
    static constexpr const char* name = "LIST_SUM";

    static function_set getFunctionSet();
};

struct ListProductFunction {
    static constexpr const char* name = "LIST_PRODUCT";

    static function_set getFunctionSet();
};

struct ListDistinctFunction {
    static constexpr const char* name = "LIST_DISTINCT";

    static function_set getFunctionSet();
};

struct ListUniqueFunction {
    static constexpr const char* name = "LIST_UNIQUE";

    static function_set getFunctionSet();
};

struct ListAnyValueFunction {
    static constexpr const char* name = "LIST_ANY_VALUE";

    static function_set getFunctionSet();
};

struct ListReverseFunction {
    static constexpr const char* name = "LIST_REVERSE";

    static function_set getFunctionSet();
};

struct ListToStringFunction {
    static constexpr const char* name = "LIST_TO_STRING";

    static function_set getFunctionSet();
};

struct ListTransformFunction {
    static constexpr const char* name = "LIST_TRANSFORM";

    static function_set getFunctionSet();
};

struct ListFilterFunction {
    static constexpr const char* name = "LIST_FILTER";

    static function_set getFunctionSet();
};

struct ListReduceFunction {
    static constexpr const char* name = "LIST_REDUCE";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
