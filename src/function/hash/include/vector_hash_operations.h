#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/function/hash/operations/include/hash_operations.h"

namespace kuzu {
namespace function {

struct UnaryHashOperationExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE>
    static void execute(ValueVector& operand, ValueVector& result) {
        result.state = operand.state;
        auto resultValues = (RESULT_TYPE*)result.values;
        auto operandValues = (OPERAND_TYPE*)operand.values;
        if (operand.state->isFlat()) {
            auto pos = operand.state->getPositionOfCurrIdx();
            if (!operand.isNull(pos)) {
                operation::Hash::operation(operandValues[pos], resultValues[pos]);
            } else {
                resultValues[pos] = operation::NULL_HASH;
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        operation::Hash::operation(operandValues[i], resultValues[i]);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        operation::Hash::operation(operandValues[pos], resultValues[pos]);
                    }
                }
            } else {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        if (!operand.isNull(i)) {
                            operation::Hash::operation(operandValues[i], resultValues[i]);
                        } else {
                            resultValues[i] = operation::NULL_HASH;
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        if (!operand.isNull(pos)) {
                            operation::Hash::operation(operandValues[pos], resultValues[pos]);
                        } else {
                            resultValues[pos] = operation::NULL_HASH;
                        }
                    }
                }
            }
        }
    }
};

struct VectorHashOperations {
    static void computeHash(ValueVector* operand, ValueVector* result);

    static void combineHash(ValueVector* left, ValueVector* right, ValueVector* result);
};

} // namespace function
} // namespace kuzu
