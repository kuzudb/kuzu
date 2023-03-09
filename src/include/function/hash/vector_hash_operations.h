#pragma once

#include "common/vector/value_vector.h"
#include "hash_operations.h"

namespace kuzu {
namespace function {

struct UnaryHashOperationExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE>
    static void execute(common::ValueVector& operand, common::ValueVector& result) {
        auto resultValues = (RESULT_TYPE*)result.getData();
        if (operand.state->isFlat()) {
            auto pos = operand.state->selVector->selectedPositions[0];
            if (!operand.isNull(pos)) {
                operation::Hash::operation(operand.getValue<OPERAND_TYPE>(pos), resultValues[pos]);
            } else {
                result.setValue(pos, operation::NULL_HASH);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        operation::Hash::operation(
                            operand.getValue<OPERAND_TYPE>(i), resultValues[i]);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        operation::Hash::operation(
                            operand.getValue<OPERAND_TYPE>(pos), resultValues[pos]);
                    }
                }
            } else {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        if (!operand.isNull(i)) {
                            operation::Hash::operation(
                                operand.getValue<OPERAND_TYPE>(i), resultValues[i]);
                        } else {
                            result.setValue(i, operation::NULL_HASH);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        if (!operand.isNull(pos)) {
                            operation::Hash::operation(
                                operand.getValue<OPERAND_TYPE>(pos), resultValues[pos]);
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
    static void computeHash(common::ValueVector* operand, common::ValueVector* result);

    static void combineHash(
        common::ValueVector* left, common::ValueVector* right, common::ValueVector* result);
};

} // namespace function
} // namespace kuzu
