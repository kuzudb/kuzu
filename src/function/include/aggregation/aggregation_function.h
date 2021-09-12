#pragma once

#include <functional>
#include <utility>

#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {

typedef void (*aggr_initialize_function_t)(uint8_t* state);
typedef void (*aggr_update_function_t)(uint8_t* state, ValueVector* input, uint64_t count);
typedef void (*aggr_combine_function_t)(uint8_t* state, uint8_t* otherState);
typedef void (*aggr_finalize_function_t)(uint8_t* inputState, uint8_t* finalState);

template<typename T>
struct AggregationState {
    T val;
};

class AggregationFunction {

public:
    AggregationFunction(aggr_initialize_function_t initializeFunc,
        aggr_update_function_t updateFunc, aggr_combine_function_t combineFunc,
        aggr_finalize_function_t finalizeFunc)
        : initializeFunc{initializeFunc}, updateFunc{updateFunc}, combineFunc{combineFunc},
          finalizeFunc{finalizeFunc} {}

    inline void initialize(uint8_t* state) { initializeFunc(state); }
    inline void update(uint8_t* state, ValueVector* input, uint64_t count) {
        updateFunc(state, input, count);
    }
    inline void combine(uint8_t* state, uint8_t* otherState) { combineFunc(state, otherState); }
    inline void finalize(uint8_t* inputState, uint8_t* finalState) {
        finalizeFunc(inputState, finalState);
    }

private:
    aggr_initialize_function_t initializeFunc;
    aggr_update_function_t updateFunc;
    aggr_combine_function_t combineFunc;
    aggr_finalize_function_t finalizeFunc;
};
} // namespace function
} // namespace graphflow
