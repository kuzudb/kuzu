#pragma once

#include "src/function/include/vector_operations.h"
#include "src/function/string/operations/include/lower_operation.h"
#include "src/function/string/operations/include/ltrim_operation.h"
#include "src/function/string/operations/include/reverse_operation.h"
#include "src/function/string/operations/include/rtrim_operation.h"
#include "src/function/string/operations/include/trim_operation.h"
#include "src/function/string/operations/include/upper_operation.h"

namespace graphflow {
namespace function {

struct VectorStringOperations : public VectorOperations {

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryStringExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 3);
        TernaryOperationExecutor::executeStringAndList<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryStringExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeStringAndList<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::executeString<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }

    template<class OPERATION>
    static inline vector<unique_ptr<VectorOperationDefinition>> getUnaryStrFunctionDefintion(
        string funcName) {
        vector<unique_ptr<VectorOperationDefinition>> definitions;
        definitions.emplace_back(make_unique<VectorOperationDefinition>(funcName,
            vector<DataTypeID>{STRING}, STRING,
            UnaryStringExecFunction<gf_string_t, gf_string_t, OPERATION>, false /* isVarLength */));
        return definitions;
    }
};

struct ContainsVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct StartsWithVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ConcatVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LowerVectorOperation : public VectorStringOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Lower>(LOWER_FUNC_NAME);
    }
};

struct UpperVectorOperation : public VectorStringOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Upper>(UPPER_FUNC_NAME);
    }
};

struct TrimVectorOperation : public VectorStringOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Trim>(TRIM_FUNC_NAME);
    }
};

struct LtrimVectorOperation : public VectorStringOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Ltrim>(LTRIM_FUNC_NAME);
    }
};

struct RtrimVectorOperation : public VectorStringOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Rtrim>(RTRIM_FUNC_NAME);
    }
};

struct ReverseVectorOperation : public VectorStringOperations {
    static inline vector<unique_ptr<VectorOperationDefinition>> getDefinitions() {
        return getUnaryStrFunctionDefintion<operation::Reverse>(REVERSE_FUNC_NAME);
    }
};

struct LengthVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct RepeatVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LpadVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct RpadVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct SubStrVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct LeftVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct RightVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ArrayExtractVectorOperation : public VectorStringOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace graphflow
