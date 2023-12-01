#pragma once

#include "common/vector/value_vector.h"
#include "hash_functions.h"

namespace kuzu {
namespace function {

struct UnaryHashFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE>
    static void execute(common::ValueVector& operand, common::ValueVector& result) {
        auto resultValues = (RESULT_TYPE*)result.getData();
        if (operand.state->isFlat()) {
            auto pos = operand.state->selVector->selectedPositions[0];
            if (!operand.isNull(pos)) {
                Hash::operation(operand.getValue<OPERAND_TYPE>(pos), resultValues[pos]);
            } else {
                result.setValue(pos, NULL_HASH);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        Hash::operation(operand.getValue<OPERAND_TYPE>(i), resultValues[i]);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        Hash::operation(operand.getValue<OPERAND_TYPE>(pos), resultValues[pos]);
                    }
                }
            } else {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        if (!operand.isNull(i)) {
                            Hash::operation(operand.getValue<OPERAND_TYPE>(i), resultValues[i]);
                        } else {
                            result.setValue(i, NULL_HASH);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        if (!operand.isNull(pos)) {
                            Hash::operation(operand.getValue<OPERAND_TYPE>(pos), resultValues[pos]);
                        } else {
                            resultValues[pos] = NULL_HASH;
                        }
                    }
                }
            }
        }
    }
};

struct VectorHashFunction {
    static void computeHash(common::ValueVector* operand, common::ValueVector* result);

    static void combineHash(
        common::ValueVector* left, common::ValueVector* right, common::ValueVector* result);
};

} // namespace function
} // namespace kuzu
