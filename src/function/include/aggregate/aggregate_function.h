#pragma once

#include <functional>
#include <utility>

#include "src/binder/include/expression/function_expression.h"
#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;
using namespace graphflow::binder;

namespace graphflow {
namespace function {

struct AggregateState {
    virtual inline uint64_t getStateSize() const = 0;
    virtual uint8_t* getResult() const = 0;

    bool isNull = true;
};

using aggr_initialize_function_t = std::function<unique_ptr<AggregateState>()>;
using aggr_update_function_t =
    std::function<void(uint8_t* state, ValueVector* input, uint64_t multiplicity)>;
using aggr_combine_function_t = std::function<void(uint8_t* state, uint8_t* otherState)>;
using aggr_finalize_function_t = std::function<void(uint8_t* state)>;

class AggregateFunction {

public:
    AggregateFunction(aggr_initialize_function_t initializeFunc, aggr_update_function_t updateFunc,
        aggr_combine_function_t combineFunc, aggr_finalize_function_t finalizeFunc,
        DataType inputDataType, bool isDistinct = false)
        : initializeFunc{move(initializeFunc)}, updateFunc{move(updateFunc)}, combineFunc{move(
                                                                                  combineFunc)},
          finalizeFunc{move(finalizeFunc)}, inputDataType{inputDataType}, isDistinct{isDistinct} {
        initialNullAggregateState = createInitialNullAggregateState();
    }

    inline uint64_t getAggregateStateSize() { return initialNullAggregateState->getStateSize(); }

    inline AggregateState* getInitialNullAggregateState() {
        return initialNullAggregateState.get();
    }

    inline unique_ptr<AggregateState> createInitialNullAggregateState() { return initializeFunc(); }

    inline void updateState(uint8_t* state, ValueVector* input, uint64_t multiplicity) {
        return updateFunc(state, input, multiplicity);
    }

    inline void combineState(uint8_t* state, uint8_t* otherState) {
        return combineFunc(state, otherState);
    }

    inline void finalizeState(uint8_t* state) { return finalizeFunc(state); }

    inline DataType getInputDataType() const { return inputDataType; }

    inline bool isFunctionDistinct() const { return isDistinct; }

    unique_ptr<AggregateFunction> clone() {
        return make_unique<AggregateFunction>(
            initializeFunc, updateFunc, combineFunc, finalizeFunc, inputDataType, isDistinct);
    }

private:
    aggr_initialize_function_t initializeFunc;
    aggr_update_function_t updateFunc;
    aggr_combine_function_t combineFunc;
    aggr_finalize_function_t finalizeFunc;

    DataType inputDataType;
    bool isDistinct;

    unique_ptr<AggregateState> initialNullAggregateState;
};

class AggregateFunctionUtil {

public:
    static unique_ptr<AggregateFunction> getAggregateFunction(Expression& expression);

private:
    static unique_ptr<AggregateFunction> getCountStarFunction();
    static unique_ptr<AggregateFunction> getCountFunction(FunctionExpression& functionExpression);
    static unique_ptr<AggregateFunction> getAvgFunction(FunctionExpression& functionExpression);
    static unique_ptr<AggregateFunction> getSumFunction(FunctionExpression& functionExpression);
    template<bool IS_MIN>
    static unique_ptr<AggregateFunction> getMinMaxFunction(FunctionExpression& functionExpression);
};

} // namespace function
} // namespace graphflow
