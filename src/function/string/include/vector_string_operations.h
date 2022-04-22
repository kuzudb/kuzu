#pragma once

#include "src/function/include/vector_operations.h"
#include "src/function/string/operations/include/lower_operation.h"
#include "src/function/string/operations/include/ltrim_operation.h"
#include "src/function/string/operations/include/reverse_operation.h"
#include "src/function/string/operations/include/rtrim_operation.h"
#include "src/function/string/operations/include/upper_operation.h"

namespace graphflow {
namespace function {

struct VectorStringOperations : public VectorOperations {
    template<class OPERATION>
    static inline vector<unique_ptr<VectorOperationDefinition>> getUnaryStrFunctionDefintion(
        string funcName) {
        vector<unique_ptr<VectorOperationDefinition>> definitions;
        auto execFunc = UnaryStringExecFunction<gf_string_t, gf_string_t, OPERATION>;
        definitions.emplace_back(make_unique<VectorOperationDefinition>(
            funcName, vector<DataTypeID>{STRING}, STRING, execFunc, false /* isVarLength */));
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

} // namespace function
} // namespace graphflow
