#pragma once

#include <functional>
#include <utility>

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct AggregateState {
    virtual inline uint32_t getStateSize() const = 0;
    virtual void moveResultToVector(common::ValueVector* outputVector, uint64_t pos) = 0;
    virtual ~AggregateState() = default;

    bool isNull = true;
};

using param_rewrite_function_t = std::function<void(binder::expression_vector&)>;
using aggr_initialize_function_t = std::function<std::unique_ptr<AggregateState>()>;
using aggr_update_all_function_t = std::function<void(uint8_t* state, common::ValueVector* input,
    uint64_t multiplicity, storage::MemoryManager* memoryManager)>;
using aggr_update_pos_function_t = std::function<void(uint8_t* state, common::ValueVector* input,
    uint64_t multiplicity, uint32_t pos, storage::MemoryManager* memoryManager)>;
using aggr_combine_function_t =
    std::function<void(uint8_t* state, uint8_t* otherState, storage::MemoryManager* memoryManager)>;
using aggr_finalize_function_t = std::function<void(uint8_t* state)>;

struct AggregateFunction final : public BaseScalarFunction {
    AggregateFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, aggr_initialize_function_t initializeFunc,
        aggr_update_all_function_t updateAllFunc, aggr_update_pos_function_t updatePosFunc,
        aggr_combine_function_t combineFunc, aggr_finalize_function_t finalizeFunc, bool isDistinct,
        scalar_bind_func bindFunc = nullptr, param_rewrite_function_t paramRewriteFunc = nullptr)
        : BaseScalarFunction{FunctionType::AGGREGATE, std::move(name), std::move(parameterTypeIDs),
              returnTypeID, std::move(bindFunc)},
          isDistinct{isDistinct}, initializeFunc{std::move(initializeFunc)},
          updateAllFunc{std::move(updateAllFunc)}, updatePosFunc{std::move(updatePosFunc)},
          combineFunc{std::move(combineFunc)}, finalizeFunc{std::move(finalizeFunc)},
          paramRewriteFunc{std::move(paramRewriteFunc)} {
        initialNullAggregateState = createInitialNullAggregateState();
    }

    AggregateFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        common::LogicalTypeID returnTypeID, aggr_initialize_function_t initializeFunc,
        aggr_update_all_function_t updateAllFunc, aggr_update_pos_function_t updatePosFunc,
        aggr_combine_function_t combineFunc, aggr_finalize_function_t finalizeFunc, bool isDistinct,
        param_rewrite_function_t paramRewriteFunc)
        : AggregateFunction{std::move(name), std::move(parameterTypeIDs), returnTypeID,
              std::move(initializeFunc), std::move(updateAllFunc), std::move(updatePosFunc),
              std::move(combineFunc), std::move(finalizeFunc), isDistinct, nullptr /* bindFunc */,
              std::move(paramRewriteFunc)} {}

    inline uint32_t getAggregateStateSize() const {
        return initialNullAggregateState->getStateSize();
    }

    // NOLINTNEXTLINE(readability-make-member-function-const): Returns a non-const pointer.
    inline AggregateState* getInitialNullAggregateState() {
        return initialNullAggregateState.get();
    }

    inline std::unique_ptr<AggregateState> createInitialNullAggregateState() const {
        return initializeFunc();
    }

    inline void updateAllState(uint8_t* state, common::ValueVector* input, uint64_t multiplicity,
        storage::MemoryManager* memoryManager) const {
        return updateAllFunc(state, input, multiplicity, memoryManager);
    }

    inline void updatePosState(uint8_t* state, common::ValueVector* input, uint64_t multiplicity,
        uint32_t pos, storage::MemoryManager* memoryManager) const {
        return updatePosFunc(state, input, multiplicity, pos, memoryManager);
    }

    inline void combineState(
        uint8_t* state, uint8_t* otherState, storage::MemoryManager* memoryManager) const {
        return combineFunc(state, otherState, memoryManager);
    }

    inline void finalizeState(uint8_t* state) const { return finalizeFunc(state); }

    inline bool isFunctionDistinct() const { return isDistinct; }

    inline std::unique_ptr<Function> copy() const override {
        return std::make_unique<AggregateFunction>(name, parameterTypeIDs, returnTypeID,
            initializeFunc, updateAllFunc, updatePosFunc, combineFunc, finalizeFunc, isDistinct,
            bindFunc, paramRewriteFunc);
    }

    inline std::unique_ptr<AggregateFunction> clone() const {
        return std::make_unique<AggregateFunction>(name, parameterTypeIDs, returnTypeID,
            initializeFunc, updateAllFunc, updatePosFunc, combineFunc, finalizeFunc, isDistinct,
            bindFunc, paramRewriteFunc);
    }

    bool isDistinct;
    aggr_initialize_function_t initializeFunc;
    aggr_update_all_function_t updateAllFunc;
    aggr_update_pos_function_t updatePosFunc;
    aggr_combine_function_t combineFunc;
    aggr_finalize_function_t finalizeFunc;
    std::unique_ptr<AggregateState> initialNullAggregateState;
    // Rewrite aggregate on NODE/REL, e.g. COUNT(a) -> COUNT(a._id)
    param_rewrite_function_t paramRewriteFunc;
};

class AggregateFunctionUtil {

public:
    template<typename T>
    static std::unique_ptr<AggregateFunction> getAggFunc(std::string name,
        common::LogicalTypeID inputType, common::LogicalTypeID resultType, bool isDistinct,
        param_rewrite_function_t paramRewriteFunc = nullptr) {
        return std::make_unique<AggregateFunction>(std::move(name),
            std::vector<common::LogicalTypeID>{inputType}, resultType, T::initialize, T::updateAll,
            T::updatePos, T::combine, T::finalize, isDistinct, paramRewriteFunc);
    }
    static std::unique_ptr<AggregateFunction> getSumFunc(const std::string name,
        common::LogicalTypeID inputType, common::LogicalTypeID resultType, bool isDistinct);
    static std::unique_ptr<AggregateFunction> getAvgFunc(const std::string name,
        common::LogicalTypeID inputType, common::LogicalTypeID resultType, bool isDistinct);
    static std::unique_ptr<AggregateFunction> getMinFunc(
        common::LogicalTypeID inputType, bool isDistinct);
    static std::unique_ptr<AggregateFunction> getMaxFunc(
        common::LogicalTypeID inputType, bool isDistinct);
    template<typename FUNC>
    static std::unique_ptr<AggregateFunction> getMinMaxFunction(const std::string name,
        common::LogicalTypeID inputType, common::LogicalTypeID resultType, bool isDistinct);
};

} // namespace function
} // namespace kuzu
