#pragma once

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct UnaryHashFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE>
    static void execute(common::ValueVector& operand, common::SelectionVector& operandSelectVec,
        common::ValueVector& result, common::SelectionVector& resultSelectVec);
};

struct BinaryHashFunctionExecutor {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeOnValue(common::ValueVector& left, common::sel_t leftPos,
        common::ValueVector& right, common::sel_t rightPos, common::ValueVector& result,
        common::sel_t resultPos) {
        FUNC::operation(left.getValue<LEFT_TYPE>(leftPos), right.getValue<RIGHT_TYPE>(rightPos),
            result.getValue<RESULT_TYPE>(resultPos));
    }

    //    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    //    static void executeBothFlat(common::ValueVector& left, common::ValueVector& right,
    //        common::ValueVector& result) {
    //        auto lPos = left.state->getSelVector()[0];
    //        auto rPos = right.state->getSelVector()[0];
    //        auto resPos = result.state->getSelVector()[0];
    //        result.setNull(resPos, left.isNull(lPos) || right.isNull(rPos));
    //        if (!result.isNull(resPos)) {
    //            executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right, result);
    //        }
    //    }
    //
    //    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    //    static void executeFlatUnFlat(common::ValueVector& left, common::ValueVector& right,
    //        common::ValueVector& result, void* dataPtr) {
    //        auto lPos = left.state->getSelVector()[0];
    //        auto& rightSelVector = right.state->getSelVector();
    //        if (left.isNull(lPos)) {
    //            result.setAllNull();
    //        } else if (right.hasNoNullsGuarantee()) {
    //            if (rightSelVector.isUnfiltered()) {
    //                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
    //                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                    result);
    //                }
    //            } else {
    //                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
    //                    auto rPos = rightSelVector[i];
    //                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                    result);
    //                }
    //            }
    //        } else {
    //            if (rightSelVector.isUnfiltered()) {
    //                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
    //                    result.setNull(i, right.isNull(i)); // left is always not null
    //                    if (!result.isNull(i)) {
    //                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                            result);
    //                    }
    //                }
    //            } else {
    //                for (auto i = 0u; i < rightSelVector.getSelSize(); ++i) {
    //                    auto rPos = rightSelVector[i];
    //                    result.setNull(rPos, right.isNull(rPos)); // left is always not null
    //                    if (!result.isNull(rPos)) {
    //                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                            result);
    //                    }
    //                }
    //            }
    //        }
    //    }
    //
    //    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    //    static void executeUnFlatFlat(common::ValueVector& left, common::ValueVector& right,
    //        common::ValueVector& result, void* dataPtr) {
    //        auto rPos = right.state->getSelVector()[0];
    //        auto& leftSelVector = left.state->getSelVector();
    //        if (right.isNull(rPos)) {
    //            result.setAllNull();
    //        } else if (left.hasNoNullsGuarantee()) {
    //            if (leftSelVector.isUnfiltered()) {
    //                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
    //                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                    result);
    //                }
    //            } else {
    //                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
    //                    auto lPos = leftSelVector[i];
    //                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                    result);
    //                }
    //            }
    //        } else {
    //            if (leftSelVector.isUnfiltered()) {
    //                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
    //                    result.setNull(i, left.isNull(i)); // right is always not null
    //                    if (!result.isNull(i)) {
    //                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                            result);
    //                    }
    //                }
    //            } else {
    //                for (auto i = 0u; i < leftSelVector.getSelSize(); ++i) {
    //                    auto lPos = leftSelVector[i];
    //                    result.setNull(lPos, left.isNull(lPos)); // right is always not null
    //                    if (!result.isNull(lPos)) {
    //                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                            result);
    //                    }
    //                }
    //            }
    //        }
    //    }
    //
    //    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    //    static void executeBothUnFlat(common::ValueVector& left, common::SelectionVector&
    //    leftSelVec,
    //        common::ValueVector& right, common::SelectionVector& rightSelVec,
    //        common::ValueVector& result, common::SelectionVector& resultSelVec) {
    //        KU_ASSERT(leftSelVec.getSelSize() == rightSelVec.getSelSize());
    //        KU_ASSERT(rightSelVec.getSelSize() == resultSelVec.getSelSize());
    //        if (left.hasNoNullsGuarantee() && right.hasNoNullsGuarantee()) {
    //            if (resultSelVec.isUnfiltered()) {
    //                for (uint64_t i = 0; i < resultSelVec.getSelSize(); i++) {
    //                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                    result);
    //                }
    //            } else {
    //                for (uint64_t i = 0; i < resultSelVec.getSelSize(); i++) {
    //                    auto pos = resultSelVector[i];
    //                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                    result);
    //                }
    //            }
    //        } else {
    //            if (resultSelVector.isUnfiltered()) {
    //                for (uint64_t i = 0; i < resultSelVector.getSelSize(); i++) {
    //                    result.setNull(i, left.isNull(i) || right.isNull(i));
    //                    if (!result.isNull(i)) {
    //                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                            result);
    //                    }
    //                }
    //            } else {
    //                for (uint64_t i = 0; i < resultSelVector.getSelSize(); i++) {
    //                    auto pos = resultSelVector[i];
    //                    result.setNull(pos, left.isNull(pos) || right.isNull(pos));
    //                    if (!result.isNull(pos)) {
    //                        executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, right,
    //                            result);
    //                    }
    //                }
    //            }
    //        }
    //    }

    static void validateSeleState(common::SelectionVector& leftSelVec,
        common::SelectionVector& rightSelVec, common::SelectionVector& resultSelVec) {
        auto leftSelSize = leftSelVec.getSelSize();
        auto rightSelSize = rightSelVec.getSelSize();
        auto resultSelSize = resultSelVec.getSelSize();
        if (leftSelSize > 1 && rightSelSize > 1) {
            KU_ASSERT(leftSelSize == rightSelSize);
            KU_ASSERT(leftSelSize == resultSelSize);
        } else if (leftSelSize > 1) {
            KU_ASSERT(leftSelSize == resultSelSize);
        } else if (rightSelSize > 1) {
            KU_ASSERT(rightSelSize == resultSelSize);
        }
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& left, common::SelectionVector& leftSelVec,
        common::ValueVector& right, common::SelectionVector& rightSelVec,
        common::ValueVector& result, common::SelectionVector& resultSelVec) {

        validateSeleState(leftSelVec, rightSelVec, resultSelVec);
        result.resetAuxiliaryBuffer();

        if (leftSelVec.getSelSize() > 1 && rightSelVec.getSelSize() > 1) {
            for (auto i = 0u; i < leftSelVec.getSelSize(); i++) {
                auto leftPos = leftSelVec[i];
                auto rightPos = rightSelVec[i];
                auto resultPos = resultSelVec[i];
                executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, leftPos, right,
                    rightPos, result, resultPos);
            }
        } else {
            for (auto i = 0u; i < leftSelVec.getSelSize(); i++) {
                for (auto j = 0u; j < rightSelVec.getSelSize(); j++) {
                    auto leftPos = leftSelVec[i];
                    auto rightPos = rightSelVec[j];
                    auto resultPos = resultSelVec[leftSelVec.getSelSize() > 1 ? i : j];
                    executeOnValue<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, leftPos, right,
                        rightPos, result, resultPos);
                }
            }
        }

        //        if (left.state->isFlat() && right.state->isFlat()) {
        //            executeBothFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, leftSelVec,
        //            right,
        //                rightSelVec, result, resultSelVec);
        //        } else if (left.state->isFlat() && !right.state->isFlat()) {
        //            executeFlatUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, leftSelVec,
        //            right,
        //                rightSelVec, result, resultSelVec);
        //        } else if (!left.state->isFlat() && right.state->isFlat()) {
        //            executeUnFlatFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, leftSelVec,
        //            right,
        //                rightSelVec, result, resultSelVec);
        //        } else if (!left.state->isFlat() && !right.state->isFlat()) {
        //            executeBothUnFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(left, leftSelVec,
        //            right,
        //                rightSelVec, result, resultSelVec);
        //        } else {
        //            KU_ASSERT(false);
        //        }
    }
};

struct VectorHashFunction {
    static void computeHash(common::ValueVector& operand, common::SelectionVector& operandSelectVec,
        common::ValueVector& result, common::SelectionVector& resultSelectVec);

    static void combineHash(common::ValueVector* left, common::ValueVector* right,
        common::ValueVector* result);

    static void combineHash(common::ValueVector& left, common::SelectionVector& leftSelVec,
        common::ValueVector& right, common::SelectionVector& rightSelVec,
        common::ValueVector& result, common::SelectionVector& resultSelVec);
};

struct MD5Function {
    static constexpr const char* name = "MD5";

    static function_set getFunctionSet();
};

struct SHA256Function {
    static constexpr const char* name = "SHA256";

    static function_set getFunctionSet();
};

struct HashFunction {
    static constexpr const char* name = "HASH";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
