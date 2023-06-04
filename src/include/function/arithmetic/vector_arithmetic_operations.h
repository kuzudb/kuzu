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
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct SubtractVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct MultiplyVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct DivideVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ModuloVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct PowerVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct AbsVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct AcosVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct AsinVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct AtanVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct Atan2VectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct BitwiseXorVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct BitwiseAndVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct BitwiseOrVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct BitShiftLeftVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct BitShiftRightVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CbrtVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CeilVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CosVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CotVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct DegreesVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct EvenVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct FactorialVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct FloorVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct GammaVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LgammaVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LnVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LogVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct Log2VectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct NegateVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct PiVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct RadiansVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct RoundVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct SinVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct SignVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct SqrtVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct TanVectorOperation : public VectorArithmeticOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace kuzu
