#pragma once

#include <cmath>

#include "common/types/int128_t.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct ArithmeticFunction {
    template<typename FUNC>
    static std::unique_ptr<ScalarFunction> getUnaryFunction(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getUnaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, operandTypeID, execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static std::unique_ptr<ScalarFunction> getUnaryFunction(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, resultTypeID,
            ScalarFunction::UnaryExecFunction<OPERAND_TYPE, RETURN_TYPE, FUNC>);
    }

    template<typename FUNC>
    static inline std::unique_ptr<ScalarFunction> getBinaryFunction(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getBinaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID}, operandTypeID,
            execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static inline std::unique_ptr<ScalarFunction> getBinaryFunction(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<ScalarFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID}, resultTypeID,
            ScalarFunction::BinaryExecFunction<OPERAND_TYPE, OPERAND_TYPE, RETURN_TYPE, FUNC>);
    }

private:
    template<typename FUNC>
    static void getUnaryExecFunc(common::LogicalTypeID operandTypeID, scalar_exec_func& func) {
        switch (operandTypeID) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = ScalarFunction::UnaryExecFunction<int64_t, int64_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT32: {
            func = ScalarFunction::UnaryExecFunction<int32_t, int32_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT16: {
            func = ScalarFunction::UnaryExecFunction<int16_t, int16_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT8: {
            func = ScalarFunction::UnaryExecFunction<int8_t, int8_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT64: {
            func = ScalarFunction::UnaryExecFunction<uint64_t, uint64_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT32: {
            func = ScalarFunction::UnaryExecFunction<uint32_t, uint32_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT16: {
            func = ScalarFunction::UnaryExecFunction<uint16_t, uint16_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT8: {
            func = ScalarFunction::UnaryExecFunction<uint8_t, uint8_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT128: {
            func = ScalarFunction::UnaryExecFunction<kuzu::common::int128_t, kuzu::common::int128_t,
                FUNC>;
        } break;
        case common::LogicalTypeID::DOUBLE: {
            func = ScalarFunction::UnaryExecFunction<double_t, double_t, FUNC>;
        } break;
        case common::LogicalTypeID::FLOAT: {
            func = ScalarFunction::UnaryExecFunction<float_t, float_t, FUNC>;
        } break;
        default:
            KU_UNREACHABLE;
        }
    }

    template<typename FUNC>
    static void getBinaryExecFunc(common::LogicalTypeID operandTypeID, scalar_exec_func& func) {
        switch (operandTypeID) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = ScalarFunction::BinaryExecFunction<int64_t, int64_t, int64_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT32: {
            func = ScalarFunction::BinaryExecFunction<int32_t, int32_t, int32_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT16: {
            func = ScalarFunction::BinaryExecFunction<int16_t, int16_t, int16_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT8: {
            func = ScalarFunction::BinaryExecFunction<int8_t, int8_t, int8_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT64: {
            func = ScalarFunction::BinaryExecFunction<uint64_t, uint64_t, uint64_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT32: {
            func = ScalarFunction::BinaryExecFunction<uint32_t, uint32_t, uint32_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT16: {
            func = ScalarFunction::BinaryExecFunction<uint16_t, uint16_t, uint16_t, FUNC>;
        } break;
        case common::LogicalTypeID::UINT8: {
            func = ScalarFunction::BinaryExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
        } break;
        case common::LogicalTypeID::INT128: {
            func = ScalarFunction::BinaryExecFunction<kuzu::common::int128_t,
                kuzu::common::int128_t, kuzu::common::int128_t, FUNC>;
        } break;
        case common::LogicalTypeID::DOUBLE: {
            func = ScalarFunction::BinaryExecFunction<double_t, double_t, double_t, FUNC>;
        } break;
        case common::LogicalTypeID::FLOAT: {
            func = ScalarFunction::BinaryExecFunction<float_t, float_t, float_t, FUNC>;
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
};

struct AddFunction {
    static function_set getFunctionSet();
};

struct SubtractFunction {
    static function_set getFunctionSet();
};

struct MultiplyFunction {
    static function_set getFunctionSet();
};

struct DivideFunction {
    static function_set getFunctionSet();
};

struct ModuloFunction {
    static function_set getFunctionSet();
};

struct PowerFunction {
    static function_set getFunctionSet();
};

struct AbsFunction {
    static function_set getFunctionSet();
};

struct AcosFunction {
    static function_set getFunctionSet();
};

struct AsinFunction {
    static function_set getFunctionSet();
};

struct AtanFunction {
    static function_set getFunctionSet();
};

struct Atan2Function {
    static function_set getFunctionSet();
};

struct BitwiseXorFunction {
    static function_set getFunctionSet();
};

struct BitwiseAndFunction {
    static function_set getFunctionSet();
};

struct BitwiseOrFunction {
    static function_set getFunctionSet();
};

struct BitShiftLeftFunction {
    static function_set getFunctionSet();
};

struct BitShiftRightFunction {
    static function_set getFunctionSet();
};

struct CbrtFunction {
    static function_set getFunctionSet();
};

struct CeilFunction {
    static function_set getFunctionSet();
};

struct CosFunction {
    static function_set getFunctionSet();
};

struct CotFunction {
    static function_set getFunctionSet();
};

struct DegreesFunction {
    static function_set getFunctionSet();
};

struct EvenFunction {
    static function_set getFunctionSet();
};

struct FactorialFunction {
    static function_set getFunctionSet();
};

struct FloorFunction {
    static function_set getFunctionSet();
};

struct GammaFunction {
    static function_set getFunctionSet();
};

struct LgammaFunction {
    static function_set getFunctionSet();
};

struct LnFunction {
    static function_set getFunctionSet();
};

struct LogFunction {
    static function_set getFunctionSet();
};

struct Log2Function {
    static function_set getFunctionSet();
};

struct NegateFunction {
    static function_set getFunctionSet();
};

struct PiFunction {
    static function_set getFunctionSet();
};

struct RadiansFunction {
    static function_set getFunctionSet();
};

struct RoundFunction {
    static function_set getFunctionSet();
};

struct SinFunction {
    static function_set getFunctionSet();
};

struct SignFunction {
    static function_set getFunctionSet();
};

struct SqrtFunction {
    static function_set getFunctionSet();
};

struct TanFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
