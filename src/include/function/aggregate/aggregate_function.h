#pragma once

#include <functional>
#include <utility>

#include "common/vector/value_vector.h"
#include "function/function_definition.h"

namespace kuzu {
namespace function {

class AggregateFunction;

struct AggregateFunctionDefinition : public FunctionDefinition {

    AggregateFunctionDefinition(std::string name, std::vector<common::DataTypeID> parameterTypeIDs,
        common::DataTypeID returnTypeID, std::unique_ptr<AggregateFunction> aggregateFunction,
        bool isDistinct)
        : FunctionDefinition{std::move(name), std::move(parameterTypeIDs), returnTypeID},
          aggregateFunction{std::move(aggregateFunction)}, isDistinct{isDistinct} {}

    std::unique_ptr<AggregateFunction> aggregateFunction;
    bool isDistinct;
};

struct AggregateState {
    virtual inline uint32_t getStateSize() const = 0;
    virtual uint8_t* getResult() const = 0;
    virtual ~AggregateState() = default;

    bool isNull = true;
};

using aggr_initialize_function_t = std::function<std::unique_ptr<AggregateState>()>;
using aggr_update_all_function_t =
    std::function<void(uint8_t* state, common::ValueVector* input, uint64_t multiplicity)>;
using aggr_update_pos_function_t = std::function<void(
    uint8_t* state, common::ValueVector* input, uint64_t multiplicity, uint32_t pos)>;
using aggr_combine_function_t = std::function<void(uint8_t* state, uint8_t* otherState)>;
using aggr_finalize_function_t = std::function<void(uint8_t* state)>;

class AggregateFunction {

public:
    AggregateFunction(aggr_initialize_function_t initializeFunc,
        aggr_update_all_function_t updateAllFunc, aggr_update_pos_function_t updatePosFunc,
        aggr_combine_function_t combineFunc, aggr_finalize_function_t finalizeFunc,
        common::DataType inputDataType, bool isDistinct = false)
        : initializeFunc{std::move(initializeFunc)}, updateAllFunc{std::move(updateAllFunc)},
          updatePosFunc{std::move(updatePosFunc)}, combineFunc{std::move(combineFunc)},
          finalizeFunc{std::move(finalizeFunc)}, inputDataType{std::move(inputDataType)},
          isDistinct{isDistinct} {
        initialNullAggregateState = createInitialNullAggregateState();
    }

    inline uint32_t getAggregateStateSize() { return initialNullAggregateState->getStateSize(); }

    inline AggregateState* getInitialNullAggregateState() {
        return initialNullAggregateState.get();
    }

    inline std::unique_ptr<AggregateState> createInitialNullAggregateState() {
        return initializeFunc();
    }

    inline void updateAllState(uint8_t* state, common::ValueVector* input, uint64_t multiplicity) {
        return updateAllFunc(state, input, multiplicity);
    }

    inline void updatePosState(
        uint8_t* state, common::ValueVector* input, uint64_t multiplicity, uint32_t pos) {
        return updatePosFunc(state, input, multiplicity, pos);
    }

    inline void combineState(uint8_t* state, uint8_t* otherState) {
        return combineFunc(state, otherState);
    }

    inline void finalizeState(uint8_t* state) { return finalizeFunc(state); }

    inline common::DataType getInputDataType() const { return inputDataType; }

    inline bool isFunctionDistinct() const { return isDistinct; }

    std::unique_ptr<AggregateFunction> clone() {
        return make_unique<AggregateFunction>(initializeFunc, updateAllFunc, updatePosFunc,
            combineFunc, finalizeFunc, inputDataType, isDistinct);
    }

private:
    aggr_initialize_function_t initializeFunc;
    aggr_update_all_function_t updateAllFunc;
    aggr_update_pos_function_t updatePosFunc;
    aggr_combine_function_t combineFunc;
    aggr_finalize_function_t finalizeFunc;

    common::DataType inputDataType;
    bool isDistinct;

    std::unique_ptr<AggregateState> initialNullAggregateState;
};

class AggregateFunctionUtil {

public:
    static std::unique_ptr<AggregateFunction> getCountStarFunction();
    static std::unique_ptr<AggregateFunction> getCountFunction(
        const common::DataType& inputType, bool isDistinct);
    static std::unique_ptr<AggregateFunction> getAvgFunction(
        const common::DataType& inputType, bool isDistinct);
    static std::unique_ptr<AggregateFunction> getSumFunction(
        const common::DataType& inputType, bool isDistinct);
    static std::unique_ptr<AggregateFunction> getMinFunction(
        const common::DataType& inputType, bool isDistinct);
    static std::unique_ptr<AggregateFunction> getMaxFunction(
        const common::DataType& inputType, bool isDistinct);

private:
    template<typename FUNC>
    static std::unique_ptr<AggregateFunction> getMinMaxFunction(
        const common::DataType& inputType, bool isDistinct);
};

} // namespace function
} // namespace kuzu
