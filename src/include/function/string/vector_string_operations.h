#pragma once

#include "function/string/operations/lower_operation.h"
#include "function/string/operations/ltrim_operation.h"
#include "function/string/operations/reverse_operation.h"
#include "function/string/operations/rtrim_operation.h"
#include "function/string/operations/trim_operation.h"
#include "function/string/operations/upper_operation.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct VectorStringOperations : public VectorOperations {

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 3);
        TernaryOperationExecutor::executeStringAndList<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeStringAndList<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::executeString<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }

    template<class OPERATION>
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>>
    getUnaryStrFunctionDefintion(std::string funcName) {
        std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
        definitions.emplace_back(std::make_unique<VectorOperationDefinition>(funcName,
            std::vector<common::DataTypeID>{common::STRING}, common::STRING,
            UnaryStringExecFunction<common::ku_string_t, common::ku_string_t, OPERATION>,
            false /* isVarLength */));
        return definitions;
    }
};

struct ArrayExtractVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ConcatVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ContainsVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct EndsWithVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct REMatchVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LeftVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LengthVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LowerVectorOperation : public VectorStringOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Lower>(common::LOWER_FUNC_NAME);
    }
};

struct LpadVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LtrimVectorOperation : public VectorStringOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Ltrim>(common::LTRIM_FUNC_NAME);
    }
};

struct RepeatVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ReverseVectorOperation : public VectorStringOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Reverse>(common::REVERSE_FUNC_NAME);
    }
};

struct RightVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct RpadVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct RtrimVectorOperation : public VectorStringOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Rtrim>(common::RTRIM_FUNC_NAME);
    }
};

struct StartsWithVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct SubStrVectorOperation : public VectorStringOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct TrimVectorOperation : public VectorStringOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Trim>(common::TRIM_FUNC_NAME);
    }
};

struct UpperVectorOperation : public VectorStringOperations {
    static inline std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Upper>(common::UPPER_FUNC_NAME);
    }
};

} // namespace function
} // namespace kuzu
