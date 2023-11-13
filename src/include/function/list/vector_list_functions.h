#pragma once

#include <cmath>

#include "common/exception/binder.h"
#include "common/types/int128_t.h"
#include "common/types/interval_t.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct ListFunction {
    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getBinaryListExecFuncSwitchRight(common::LogicalType rightType) {
        scalar_exec_func execFunc;
        switch (rightType.getPhysicalType()) {
        case common::PhysicalTypeID::BOOL: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, uint8_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT64: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, int64_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, int32_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, int16_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, int8_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT64: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, uint64_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT32: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, uint32_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT16: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, uint16_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT8: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, uint8_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT128: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t,
                common::int128_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::DOUBLE: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, double_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::FLOAT: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t, float_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::STRING: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t,
                common::ku_string_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INTERVAL: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t,
                common::interval_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INTERNAL_ID: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t,
                common::internalID_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::VAR_LIST: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t,
                common::list_entry_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::STRUCT: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<common::list_entry_t,
                common::struct_entry_t, RESULT_TYPE, OPERATION>;
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
        return execFunc;
    }

    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getBinaryListExecFuncSwitchAll(common::LogicalType type) {
        scalar_exec_func execFunc;
        switch (type.getPhysicalType()) {
        case common::PhysicalTypeID::INT64: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<int64_t, int64_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<int32_t, int32_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<int16_t, int16_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc = ScalarFunction::BinaryExecListStructFunction<int8_t, int8_t, RESULT_TYPE,
                OPERATION>;
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
        return execFunc;
    }

    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getTernaryListExecFuncSwitchAll(common::LogicalType type) {
        scalar_exec_func execFunc;
        switch (type.getPhysicalType()) {
        case common::PhysicalTypeID::INT64: {
            execFunc = ScalarFunction::TernaryExecListStructFunction<int64_t, int64_t, int64_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc = ScalarFunction::TernaryExecListStructFunction<int32_t, int32_t, int32_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc = ScalarFunction::TernaryExecListStructFunction<int16_t, int16_t, int16_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc = ScalarFunction::TernaryExecListStructFunction<int8_t, int8_t, int8_t,
                RESULT_TYPE, OPERATION>;
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
        return execFunc;
    }

    template<typename OPERATION>
    static std::unique_ptr<FunctionBindData> bindFuncListAggr(
        const binder::expression_vector& arguments, Function* function) {
        auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
        auto resultType = common::VarListType::getChildType(&arguments[0]->dataType);
        switch (resultType->getLogicalTypeID()) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, int64_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::INT32: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, int32_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::INT16: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, int16_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::INT8: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, int8_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::UINT64: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, uint64_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::UINT32: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, uint32_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::UINT16: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, uint16_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::UINT8: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, uint8_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::INT128: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, common::int128_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::DOUBLE: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, double_t,
                    OPERATION>;
        } break;
        case common::LogicalTypeID::FLOAT: {
            scalarFunction->execFunc =
                ScalarFunction::UnaryExecListStructFunction<common::list_entry_t, float_t,
                    OPERATION>;
        } break;
        default: {
            throw common::BinderException(
                common::stringFormat("Unsupported inner data type for {}: {}", function->name,
                    common::LogicalTypeUtils::toString(resultType->getLogicalTypeID())));
        }
        }
        return std::make_unique<FunctionBindData>(*resultType);
    }
};

struct ListCreationFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/ = nullptr);
};

struct ListRangeFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct SizeFunction {
    static function_set getFunctionSet();
};

struct ListExtractFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListConcatFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListAppendFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListPrependFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListPositionFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListContainsFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListSliceFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListSortFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListReverseSortFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListSumFunction {
    static function_set getFunctionSet();
};

struct ListProductFunction {
    static function_set getFunctionSet();
};

struct ListDistinctFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListUniqueFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct ListAnyValueFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

} // namespace function
} // namespace kuzu
