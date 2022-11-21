#pragma once

#include <functional>

#include "common/type_utils.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct TernaryOperationWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(
        A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result, void* dataptr) {
        OP::operation(a, b, c, result);
    }
};

struct TernaryStringAndListOperationWrapper {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename OP>
    static inline void operation(
        A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result, void* dataptr) {
        OP::operation(a, b, c, result, *(ValueVector*)dataptr);
    }
};

struct TernaryOperationExecutor {
    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeOnValue(ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result,
        uint64_t aPos, uint64_t bPos, uint64_t cPos, uint64_t resPos) {
        auto resValues = (RESULT_TYPE*)result.getData();
        OP_WRAPPER::template operation<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            ((A_TYPE*)a.getData())[aPos], ((B_TYPE*)b.getData())[bPos],
            ((C_TYPE*)c.getData())[cPos], resValues[resPos], (void*)&result);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeAllFlat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        result.state = a.state;
        auto aPos = a.state->getPositionOfCurrIdx();
        auto bPos = b.state->getPositionOfCurrIdx();
        auto cPos = c.state->getPositionOfCurrIdx();
        auto resPos = result.state->getPositionOfCurrIdx();
        result.setNull(resPos, a.isNull(aPos) || b.isNull(bPos) || c.isNull(cPos));
        if (!result.isNull(resPos)) {
            executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result, aPos, bPos, cPos, resPos);
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatFlatUnflat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        result.state = c.state;
        auto aPos = a.state->getPositionOfCurrIdx();
        auto bPos = b.state->getPositionOfCurrIdx();
        if (a.isNull(aPos) || b.isNull(bPos)) {
            result.setAllNull();
        } else if (c.hasNoNullsGuarantee()) {
            if (c.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, bPos, i, i);
                }
            } else {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    auto pos = c.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, bPos, pos, pos);
                }
            }
        } else {
            if (c.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    result.setNull(i, c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, bPos, i, i);
                    }
                }
            } else {
                for (auto i = 0u; i < c.state->selVector->selectedSize; ++i) {
                    auto pos = c.state->selVector->selectedPositions[i];
                    result.setNull(pos, c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, bPos, pos, pos);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatUnflat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        assert(b.state == c.state);
        result.state = b.state;
        auto aPos = a.state->getPositionOfCurrIdx();
        if (a.isNull(aPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, i, i, i);
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, pos, pos, pos);
                }
            }
        } else {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    result.setNull(i, b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, i, i, i);
                    }
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    result.setNull(pos, b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, pos, pos, pos);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeFlatUnflatFlat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        result.state = b.state;
        auto aPos = a.state->getPositionOfCurrIdx();
        auto cPos = c.state->getPositionOfCurrIdx();
        if (a.isNull(aPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (b.hasNoNullsGuarantee()) {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, i, cPos, i);
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, aPos, pos, cPos, pos);
                }
            }
        } else {
            if (b.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    result.setNull(i, b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, i, cPos, i);
                    }
                }
            } else {
                for (auto i = 0u; i < b.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    result.setNull(pos, b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, aPos, pos, cPos, pos);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeAllUnFlat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        assert(a.state == b.state && b.state == c.state);
        result.state = a.state;
        if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, i, i, i);
                }
            } else {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, pos, pos, pos);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    result.setNull(i, a.isNull(i) || b.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, i, i, i);
                    }
                }
            } else {
                for (uint64_t i = 0; i < a.state->selVector->selectedSize; i++) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, pos, pos, pos);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatFlat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        result.state = a.state;
        auto bPos = b.state->getPositionOfCurrIdx();
        auto cPos = c.state->getPositionOfCurrIdx();
        if (b.isNull(bPos) || c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, bPos, cPos, i);
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, bPos, cPos, pos);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    result.setNull(i, a.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, bPos, cPos, i);
                    }
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, bPos, cPos, pos);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatFlatUnflat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        assert(a.state == c.state);
        result.state = a.state;
        auto bPos = b.state->getPositionOfCurrIdx();
        if (b.isNull(bPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && c.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, bPos, i, i);
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, bPos, pos, pos);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    result.setNull(i, a.isNull(i) || c.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, bPos, i, i);
                    }
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = b.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos) || c.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, bPos, pos, pos);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeUnflatUnFlatFlat(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        assert(a.state == b.state);
        result.state = a.state;
        auto cPos = c.state->getPositionOfCurrIdx();
        if (c.isNull(cPos)) {
            result.setAllNull();
        } else if (a.hasNoNullsGuarantee() && b.hasNoNullsGuarantee()) {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, i, i, cPos, i);
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                        a, b, c, result, pos, pos, cPos, pos);
                }
            }
        } else {
            if (a.state->selVector->isUnfiltered()) {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    result.setNull(i, a.isNull(i) || b.isNull(i));
                    if (!result.isNull(i)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, i, i, cPos, i);
                    }
                }
            } else {
                for (auto i = 0u; i < a.state->selVector->selectedSize; ++i) {
                    auto pos = a.state->selVector->selectedPositions[i];
                    result.setNull(pos, a.isNull(pos) || b.isNull(pos));
                    if (!result.isNull(pos)) {
                        executeOnValue<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            a, b, c, result, pos, pos, cPos, pos);
                    }
                }
            }
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC,
        typename OP_WRAPPER>
    static void executeSwitch(ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        result.resetOverflowBuffer();
        if (a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeAllFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(a, b, c, result);
        } else if (a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeFlatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result);
        } else if (a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeFlatUnflatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result);
        } else if (a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeFlatUnflatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result);
        } else if (!a.state->isFlat() && !b.state->isFlat() && !c.state->isFlat()) {
            executeAllUnFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result);
        } else if (!a.state->isFlat() && !b.state->isFlat() && c.state->isFlat()) {
            executeUnflatUnFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result);
        } else if (!a.state->isFlat() && b.state->isFlat() && c.state->isFlat()) {
            executeUnflatFlatFlat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result);
        } else if (!a.state->isFlat() && b.state->isFlat() && !c.state->isFlat()) {
            executeUnflatFlatUnflat<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                a, b, c, result);
        } else {
            assert(false);
        }
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC, TernaryOperationWrapper>(
            a, b, c, result);
    }

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeStringAndList(
        ValueVector& a, ValueVector& b, ValueVector& c, ValueVector& result) {
        executeSwitch<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC,
            TernaryStringAndListOperationWrapper>(a, b, c, result);
    }
};

} // namespace function
} // namespace kuzu
