#pragma once

#include "function/unary_function_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct CastFixedListHelper {
    static bool containsListToFixedList(const LogicalType* srcType, const LogicalType* dstType);

    static void validateListEntry(ValueVector* inputVector, LogicalType* resultType, uint64_t pos);
};

struct CastFixedList {
    template<typename EXECUTOR = UnaryFunctionExecutor>
    static void fixedListToStringCastExecFunction(
        const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
        void* dataPtr);

    template<typename EXECUTOR = UnaryFunctionExecutor>
    static void stringtoFixedListCastExecFunction(
        const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
        void* dataPtr);

    template<typename EXECUTOR = UnaryFunctionExecutor>
    static void listToFixedListCastExecFunction(
        const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
        void* dataPtr);

    template<typename EXECUTOR = UnaryFunctionExecutor>
    static void castBetweenFixedListExecFunc(
        const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result,
        void* dataPtr);
};

template<>
void CastFixedList::fixedListToStringCastExecFunction<UnaryFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::fixedListToStringCastExecFunction<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::fixedListToStringCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);

template<>
void CastFixedList::stringtoFixedListCastExecFunction<UnaryFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::stringtoFixedListCastExecFunction<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::stringtoFixedListCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);

template<>
void CastFixedList::listToFixedListCastExecFunction<UnaryFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::listToFixedListCastExecFunction<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::listToFixedListCastExecFunction<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);

template<>
void CastFixedList::castBetweenFixedListExecFunc<UnaryFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::castBetweenFixedListExecFunc<CastFixedListToListFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);
template<>
void CastFixedList::castBetweenFixedListExecFunc<CastChildFunctionExecutor>(
    const std::vector<std::shared_ptr<ValueVector>>& params, ValueVector& result, void* dataPtr);

} // namespace function
} // namespace kuzu
