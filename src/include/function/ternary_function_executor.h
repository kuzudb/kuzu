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
        result.setNull(resPos, a.isNull(aPos) || b.isNull(bPos) || c.isNull(cPos));
        if (!result.isNull(resPos)) {
            executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b, *c,
                *result, aPos, bPos, cPos, resPos, dataPtr);
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatFlatUnflat(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        auto aPos = (*a.sel)[0];
        auto bPos = (*b.sel)[0];
        auto& cSelVector = *c.sel;
        if (a.isNull(aPos) || b.isNull(bPos)) {
            result.setAllNull();
        } else if (c.hasNoNullsGuarantee()) {
            if (cSelVector.isUnfiltered()) {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, aPos, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    auto pos = cSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, aPos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (cSelVector.isUnfiltered()) {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    result.setNull(i, c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, aPos, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < cSelVector.getSelSize(); ++i) {
                    auto pos = cSelVector[i];
                    result.setNull(pos, c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, aPos, bPos, pos, pos, dataPtr);
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
        if (a.isNull(aPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, aPos, i, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, aPos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    result.setNull(i, b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, aPos, i, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    result.setNull(pos, b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, aPos, pos, pos, pos, dataPtr);
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
        if (a.isNull(aPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee()) {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, aPos, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, aPos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (bSelVector.isUnfiltered()) {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    result.setNull(i, b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, aPos, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < bSelVector.getSelSize(); ++i) {
                    auto pos = bSelVector[i];
                    result.setNull(pos, b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, aPos, pos, cPos, pos, dataPtr);
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
        if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, i, i, i, i, dataPtr);
                }
            } else {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, pos, pos, pos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    result.setNull(i, a.isNull(i) || b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, i, i, i, i, dataPtr);
                    }
                }
            } else {
                for (uint64_t i = 0; i < aSelVector.getSelSize(); i++) {
                    auto pos = aSelVector[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, pos, pos, pos, pos, dataPtr);
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
        if (b.isNull(bPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, i, bPos, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, pos, bPos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    result.setNull(i, a.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, i, bPos, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    result.setNull(pos, a.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, pos, bPos, cPos, pos, dataPtr);
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
        if (b.isNull(bPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, i, bPos, i, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, pos, bPos, pos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    result.setNull(i, a.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, i, bPos, i, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = (*b.sel)[i];
                    result.setNull(pos, a.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, pos, bPos, pos, pos, dataPtr);
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
        if (c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee()) {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, i, i, cPos, i, dataPtr);
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a, *b,
                        *c, *result, pos, pos, cPos, pos, dataPtr);
                }
            }
        } else {
            if (aSelVector.isUnfiltered()) {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    result.setNull(i, a.isNull(i) || b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, i, i, cPos, i, dataPtr);
                    }
                }
            } else {
                for (auto i = 0u; i < aSelVector.getSelSize(); ++i) {
                    auto pos = aSelVector[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(*a,
                            *b, *c, *result, pos, pos, cPos, pos, dataPtr);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(common::SelectedVector a, common::SelectedVector b,
        common::SelectedVector c, common::SelectedVector result, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeAllFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c, result,
                dataPtr);
        } else if (a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeFlatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeFlatUnflatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeFlatUnflatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (!a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeAllUnFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c, result,
                dataPtr);
        } else if (!a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeUnflatUnFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (!a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeUnflatFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else if (!a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeUnflatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c,
                result, dataPtr);
        } else {
            KU_ASSERT(false);
        }
    }
};

} // namespace function
} // namespace kuzu
