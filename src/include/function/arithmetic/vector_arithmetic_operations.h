#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

class VectorArithmeticOperations : public VectorOperations {

public:
    template<typename FUNC, bool INT_RESULT = false, bool DOUBLE_RESULT = false>
    static std::unique_ptr<VectorOperationDefinition> getUnaryDefinition(
        std::string name, common::DataTypeID operandTypeID, common::DataTypeID resultTypeID) {
        return make_unique<VectorOperationDefinition>(std::move(name),
            std::vector<common::DataTypeID>{operandTypeID}, resultTypeID,
            getUnaryExecFunc<FUNC, INT_RESULT, DOUBLE_RESULT>(operandTypeID));
    }

    template<typename FUNC, bool DOUBLE_RESULT = false>
    static inline std::unique_ptr<VectorOperationDefinition> getBinaryDefinition(std::string name,
        common::DataTypeID leftTypeID, common::DataTypeID rightTypeID,
        common::DataTypeID resultTypeID) {
        return make_unique<VectorOperationDefinition>(std::move(name),
            std::vector<common::DataTypeID>{leftTypeID, rightTypeID}, resultTypeID,
            getBinaryExecFunc<FUNC, DOUBLE_RESULT>(leftTypeID, rightTypeID));
    }

private:
    template<typename FUNC, bool DOUBLE_RESULT>
    static scalar_exec_func getBinaryExecFunc(
        common::DataTypeID leftTypeID, common::DataTypeID rightTypeID) {
        switch (leftTypeID) {
        case common::INT64: {
            switch (rightTypeID) {
            case common::INT64: {
                if constexpr (DOUBLE_RESULT) {
                    return BinaryExecFunction<int64_t, int64_t, double_t, FUNC>;
                } else {
                    return BinaryExecFunction<int64_t, int64_t, int64_t, FUNC>;
                }
            }
            case common::DOUBLE: {
                return BinaryExecFunction<int64_t, double_t, double_t, FUNC>;
            }
            default:
                throw common::RuntimeException("Invalid input data types(" +
                                               common::Types::dataTypeToString(leftTypeID) + "," +
                                               common::Types::dataTypeToString(rightTypeID) +
                                               ") for getBinaryExecFunc.");
            }
        }
        case common::DOUBLE: {
            switch (rightTypeID) {
            case common::INT64: {
                return BinaryExecFunction<double_t, int64_t, double_t, FUNC>;
            }
            case common::DOUBLE: {
                return BinaryExecFunction<double_t, double_t, double_t, FUNC>;
            }
            default:
                throw common::RuntimeException("Invalid input data types(" +
                                               common::Types::dataTypeToString(leftTypeID) + "," +
                                               common::Types::dataTypeToString(rightTypeID) +
                                               ") for getBinaryExecFunc.");
            }
        }
        default:
            throw common::RuntimeException(
                "Invalid input data types(" + common::Types::dataTypeToString(leftTypeID) + "," +
                common::Types::dataTypeToString(rightTypeID) + ") for getBinaryExecFunc.");
        }
    }

    template<typename FUNC, bool INT_RESULT, bool DOUBLE_RESULT>
    static scalar_exec_func getUnaryExecFunc(common::DataTypeID operandTypeID) {
        switch (operandTypeID) {
        case common::INT64: {
            if constexpr (DOUBLE_RESULT) {
                return UnaryExecFunction<int64_t, double_t, FUNC>;
            } else {
                return UnaryExecFunction<int64_t, int64_t, FUNC>;
            }
        }
        case common::DOUBLE: {
            if constexpr (INT_RESULT) {
                return UnaryExecFunction<double_t, int64_t, FUNC>;
            } else {
                return UnaryExecFunction<double_t, double_t, FUNC>;
            }
        }
        default:
            throw common::RuntimeException("Invalid input data types(" +
                                           common::Types::dataTypeToString(operandTypeID) +
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
