#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct TernaryFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* /*resultValueVector*/, void* /*dataPtr*/) {
        OP::operation(a, b, c, result);
    }
};

struct TernaryStringFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* resultValueVector, void* /*dataPtr*/) {
        OP::operation(a, b, c, result, *(common::ValueVector*)resultValueVector);
    }
};

struct TernaryRegexFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* resultValueVector, void* dataPtr) {
        OP::operation(a, b, c, result, *(common::ValueVector*)resultValueVector, dataPtr);
    }
};

struct TernaryListFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* aValueVector, void* resultValueVector, void* /*dataPtr*/) {
        OP::operation(a, b, c, result, *(common::ValueVector*)aValueVector,
            *(common::ValueVector*)resultValueVector);
    }
};

struct TernaryUDFFunctionWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* /*aValueVector*/, void* /*resultValueVector*/, void* dataPtr) {
        OP::operation(a, b, c, result, dataPtr);
    }
};

struct TernaryFunctionExecutor {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeOnValue(common::ValueVector& a, common::ValueVector& b,
        common::ValueVector& c, common::ValueVector& result, uint64_t aPos, uint64_t bPos,
        uint64_t cPos, uint64_t resPos, void* dataPtr) {
        auto resValues = (RESULT_TYPE*)result.getData();
        OP_WRAPPER::template operation<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            ((A_TYPE*)a.getData())[aPos], ((B_TYPE*)b.getData())[bPos],
            ((C_TYPE*)c.getData())[cPos], resValues[resPos], (void*)&a, (void*)&result, dataPtr);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeAllFlat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        auto aPos = (*a.sel)[0];
        auto bPos = (*b.sel)[0];
        auto cPos = (*c.sel)[0];
        auto resPos = (*result.sel)[0];
        result.vec.setNull(resPos, a.vec.isNull(aPos) || b.vec.isNull(bPos) || c.vec.isNull(cPos));
        if (!result.vec.isNull(resPos)) {
            executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec, b.vec,
                c.vec, result.vec, aPos, bPos, cPos, resPos, dataPtr);
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatFlatUnflat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        auto aPos = (*a.sel)[0];
        auto bPos = (*b.sel)[0];
        auto& cSelVector = *c.sel;
        if (a.vec.isNull(aPos) || b.vec.isNull(bPos)) {
            result.vec.setAllNull();
        } else if (c.vec.hasNoNullsGuarantee()) {
            if (cSelVector.isUnfiltered()) {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, aPos, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    auto pos = cSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, aPos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (cSelVector.isUnfiltered()) {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    result.vec.setNull(i, c.vec.isNull(i));
                    if (!result.vec.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, aPos, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    auto pos = cSelVector[i];
                    result.vec.setNull(pos, c.vec.isNull(pos));
                    if (!result.vec.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, aPos, bPos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatUnflat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(b.sel == c.sel);
        auto aPos = (*a.sel)[0];
        auto& bSelVector = *b.sel;
        if (a.vec.isNull(aPos)) {
            result.vec.setAllNull();
        } else if (b.vec.hasNoNullsGuarantee() && c.vec.hasNoNullsGuarantee()) {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, aPos, i, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, aPos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    result.vec.setNull(i, b.vec.isNull(i) || c.vec.isNull(i));
                    if (!result.vec.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, aPos, i, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    result.vec.setNull(pos, b.vec.isNull(pos) || c.vec.isNull(pos));
                    if (!result.vec.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, aPos, pos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatFlat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        auto aPos = (*a.sel)[0];
        auto cPos = (*c.sel)[0];
        auto& bSelVector = *b.sel;
        if (a.vec.isNull(aPos) || c.vec.isNull(cPos)) {
            result.vec.setAllNull();
        } else if (b.vec.hasNoNullsGuarantee()) {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, aPos, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, aPos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    result.vec.setNull(i, b.vec.isNull(i));
                    if (!result.vec.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, aPos, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    result.vec.setNull(pos, b.vec.isNull(pos));
                    if (!result.vec.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, aPos, pos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeAllUnFlat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(a.sel == b.sel && b.sel == c.sel);
        auto& aSelVector = *a.sel;
        if (a.vec.hasNoNullsGuarantee() && b.vec.hasNoNullsGuarantee() &&
            c.vec.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, i, i, i, i, dataPtr);
                }
            } else {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, pos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    result.vec.setNull(i, a.vec.isNull(i) || b.vec.isNull(i) || c.vec.isNull(i));
                    if (!result.vec.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, i, i, i, i, dataPtr);
                    }
                }
            } else {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    auto pos = aSelVector[i];
                    result.vec.setNull(pos,
                        a.vec.isNull(pos) || b.vec.isNull(pos) || c.vec.isNull(pos));
                    if (!result.vec.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, pos, pos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatFlat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        auto bPos = (*b.sel)[0];
        auto cPos = (*c.sel)[0];
        auto& aSelVector = *a.sel;
        if (b.vec.isNull(bPos) || c.vec.isNull(cPos)) {
            result.vec.setAllNull();
        } else if (a.vec.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, i, bPos, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, pos, bPos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    result.vec.setNull(i, a.vec.isNull(i));
                    if (!result.vec.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, i, bPos, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    result.vec.setNull(pos, a.vec.isNull(pos));
                    if (!result.vec.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, pos, bPos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatUnflat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(a.sel == c.sel);
        auto& aSelVector = *a.sel;
        auto bPos = (*b.sel)[0];
        if (b.vec.isNull(bPos)) {
            result.vec.setAllNull();
        } else if (a.vec.hasNoNullsGuarantee() && c.vec.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, i, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, pos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    result.vec.setNull(i, a.vec.isNull(i) || c.vec.isNull(i));
                    if (!result.vec.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, i, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = (*b.sel)[i];
                    result.vec.setNull(pos, a.vec.isNull(pos) || c.vec.isNull(pos));
                    if (!result.vec.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, pos, bPos, pos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatUnFlatFlat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        KU_ASSERT(a.sel == b.sel);
        auto& aSelVector = *a.sel;
        auto cPos = (*c.sel)[0];
        if (c.vec.isNull(cPos)) {
            result.vec.setAllNull();
        } else if (a.vec.hasNoNullsGuarantee() && b.vec.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, i, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                        b.vec, c.vec, result.vec, pos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    result.vec.setNull(i, a.vec.isNull(i) || b.vec.isNull(i));
                    if (!result.vec.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, i, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    result.vec.setNull(pos, a.vec.isNull(pos) || b.vec.isNull(pos));
                    if (!result.vec.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a.vec,
                            b.vec, c.vec, result.vec, pos, pos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        result.vec.resetAuxiliaryBuffer();
        if (a.vec.state->isFlat() && b.vec.state->isFlat() && c.vec.state->isFlat()) {
            executeAllFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c, result,
                dataPtr);
        } else if (a.vec.state->isFlat() && b.vec.state->isFlat() && !c.vec.state->isFlat()) {
            executeFlatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (a.vec.state->isFlat() && !b.vec.state->isFlat() && !c.vec.state->isFlat()) {
            executeFlatUnflatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (a.vec.state->isFlat() && !b.vec.state->isFlat() && c.vec.state->isFlat()) {
            executeFlatUnflatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (!a.vec.state->isFlat() && !b.vec.state->isFlat() && !c.vec.state->isFlat()) {
            executeAllUnFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c, result,
                dataPtr);
        } else if (!a.vec.state->isFlat() && !b.vec.state->isFlat() && c.vec.state->isFlat()) {
            executeUnflatUnFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (!a.vec.state->isFlat() && b.vec.state->isFlat() && c.vec.state->isFlat()) {
            executeUnflatFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (!a.vec.state->isFlat() && b.vec.state->isFlat() && !c.vec.state->isFlat()) {
            executeUnflatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else {
            KU_ASSERT(false);
        }
    }
};

} // namespace function
} // namespace kuzu
