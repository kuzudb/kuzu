#pragma once

#include "arithmetic_functions.h"
#include "function/vector_functions.h"

namespace kuzu {
namespace function {

class VectorArithmeticFunction : public VectorFunction {
public:
    template<typename FUNC>
    static std::unique_ptr<VectorFunctionDefinition> getUnaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getUnaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<VectorFunctionDefinition>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, operandTypeID, execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static std::unique_ptr<VectorFunctionDefinition> getUnaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<VectorFunctionDefinition>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, resultTypeID,
            UnaryExecFunction<OPERAND_TYPE, RETURN_TYPE, FUNC>);
    }

    template<typename FUNC>
    static inline std::unique_ptr<VectorFunctionDefinition> getBinaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getBinaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<VectorFunctionDefinition>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID}, operandTypeID,
            execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static inline std::unique_ptr<VectorFunctionDefinition> getBinaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<VectorFunctionDefinition>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID}, resultTypeID,
            BinaryExecFunction<OPERAND_TYPE, OPERAND_TYPE, RETURN_TYPE, FUNC>);
    }

private:
    template<typename FUNC>
    static void getUnaryExecFunc(common::LogicalTypeID operandTypeID, scalar_exec_func& func) {
        switch (operandTypeID) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = UnaryExecFunction<int64_t, int64_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = UnaryExecFunction<int32_t, int32_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = UnaryExecFunction<int16_t, int16_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = UnaryExecFunction<double_t, double_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = UnaryExecFunction<float_t, float_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::LogicalTypeUtils::dataTypeToString(operandTypeID) +
                ") for getUnaryExecFunc.");
        }
    }

    template<typename FUNC>
    static void getBinaryExecFunc(common::LogicalTypeID operandTypeID, scalar_exec_func& func) {
        switch (operandTypeID) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = BinaryExecFunction<int64_t, int64_t, int64_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = BinaryExecFunction<int32_t, int32_t, int32_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = BinaryExecFunction<int16_t, int16_t, int16_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = BinaryExecFunction<double_t, double_t, double_t, FUNC>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = BinaryExecFunction<float_t, float_t, float_t, FUNC>;
            return;
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" +
                common::LogicalTypeUtils::dataTypeToString(operandTypeID) +
                ") for getUnaryExecFunc.");
        }
    }
};

struct AddVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct SubtractVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct MultiplyVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct DivideVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct ModuloVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct PowerVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct AbsVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct AcosVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct AsinVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct AtanVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct Atan2VectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct BitwiseXorVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct BitwiseAndVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct BitwiseOrVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct BitShiftLeftVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct BitShiftRightVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct CbrtVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct CeilVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct CosVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct CotVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct DegreesVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct EvenVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct FactorialVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct FloorVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct GammaVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct LgammaVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct LnVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct LogVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct Log2VectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct NegateVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct PiVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct RadiansVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct RoundVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct SinVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct SignVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct SqrtVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

struct TanVectorFunction : public VectorArithmeticFunction {
    static vector_function_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
