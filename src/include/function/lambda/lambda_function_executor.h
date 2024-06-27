#pragma once

#include "common/vector/value_vector.h"
#include "expression_evaluator/function_evaluator.h"
#include "expression_evaluator/literal_evaluator.h"
#include "expression_evaluator/reference_evaluator.h"
#include "function/cast/cast_function_bind_data.h"
#include "function/lambda/lambda_function_bind_data.h"
#include "parser/parsed_expression_visitor.h"
#include "processor/expression_mapper.h"

namespace kuzu {
namespace function {

struct LambdaFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE>
    static void execute(const std::vector<std::shared_ptr<common::ValueVector>>& operand,
        common::ValueVector& result, void* dataPtr) {
        auto lambdaFuncBindData = reinterpret_cast<LambdaFunctionBindData*>(dataPtr);
        KU_ASSERT(operand.size() == 1);
        // Input
        auto dataVec = common::ListVector::getSharedDataVector(operand[0].get());
        dataVec->state->getSelVectorUnsafe().setSelSize(
            common::ListVector::getDataVectorSize(operand[0].get()));
        lambdaFuncBindData->evaluator->evaluate();
        memcpy(result.getData(), operand[0]->getData(),
            (operand[0]->state->getSelVector()[operand[0]->state->getSelVector().getSelSize() - 1] +
                1) *
                operand[0]->getNumBytesPerValue());
        result.state = operand[0]->state;
        common::ListVector::setDataVector(&result, lambdaFuncBindData->evaluator->resultVector);
    }
};

} // namespace function
} // namespace kuzu
