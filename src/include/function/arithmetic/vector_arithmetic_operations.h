#pragma once

#include "arithmetic_operations.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

class VectorArithmeticOperations : public VectorOperations {
public:
    template<typename FUNC>
    static std::unique_ptr<VectorOperationDefinition> getUnaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getUnaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<VectorOperationDefinition>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, operandTypeID, execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static std::unique_ptr<VectorOperationDefinition> getUnaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<VectorOperationDefinition>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID}, resultTypeID,
            UnaryExecFunction<OPERAND_TYPE, RETURN_TYPE, FUNC>);
    }

    template<typename FUNC>
    static inline std::unique_ptr<VectorOperationDefinition> getBinaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID) {
        function::scalar_exec_func execFunc;
        getBinaryExecFunc<FUNC>(operandTypeID, execFunc);
        return std::make_unique<VectorOperationDefinition>(std::move(name),
            std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID}, operandTypeID,
            execFunc);
    }

    template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
    static inline std::unique_ptr<VectorOperationDefinition> getBinaryDefinition(
        std::string name, common::LogicalTypeID operandTypeID, common::LogicalTypeID resultTypeID) {
        return std::make_unique<VectorOperationDefinition>(std::move(name),
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

struct AddVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct SubtractVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct MultiplyVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct DivideVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct ModuloVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct PowerVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct AbsVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct AcosVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct AsinVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct AtanVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct Atan2VectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct BitwiseXorVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct BitwiseAndVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct BitwiseOrVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct BitShiftLeftVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct BitShiftRightVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct CbrtVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct CeilVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct CosVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct CotVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct DegreesVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct EvenVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct FactorialVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct FloorVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct GammaVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct LgammaVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct LnVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct LogVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct Log2VectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct NegateVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct PiVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct RadiansVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct RoundVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct SinVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct SignVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct SqrtVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

struct TanVectorOperation : public VectorArithmeticOperations {
    static vector_operation_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
