#pragma once

#include <functional>
#include <utility>

#include "src/common/include/types.h"
#include "src/common/include/vector/value_vector.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {

struct AggregationState {
    virtual inline uint64_t getValSize() const = 0;
    virtual uint8_t* getFinalVal() const = 0;

    bool isNull = true;
};

using aggr_initialize_function_t = std::function<unique_ptr<AggregationState>()>;
using aggr_update_function_t =
    std::function<void(uint8_t* state, ValueVector* input, uint64_t count)>;
using aggr_combine_function_t = std::function<void(uint8_t* state, uint8_t* otherState)>;
using aggr_finalize_function_t = std::function<void(uint8_t* state)>;

class AggregationFunction {

public:
    AggregationFunction(aggr_initialize_function_t initializeFunc,
        aggr_update_function_t updateFunc, aggr_combine_function_t combineFunc,
        aggr_finalize_function_t finalizeFunc)
        : initializeFunc{move(initializeFunc)}, updateFunc{move(updateFunc)},
          combineFunc{move(combineFunc)}, finalizeFunc{move(finalizeFunc)} {}

    inline unique_ptr<AggregationState> initialize() { return initializeFunc(); }
    inline void update(uint8_t* state, ValueVector* input, uint64_t count) {
        updateFunc(state, input, count);
    }
    inline void combine(uint8_t* state, uint8_t* otherState) { combineFunc(state, otherState); }
    inline void finalize(uint8_t* state) { finalizeFunc(state); }

    unique_ptr<AggregationFunction> clone() {
        return make_unique<AggregationFunction>(
            initializeFunc, updateFunc, combineFunc, finalizeFunc);
    }

private:
    aggr_initialize_function_t initializeFunc;
    aggr_update_function_t updateFunc;
    aggr_combine_function_t combineFunc;
    aggr_finalize_function_t finalizeFunc;
};
} // namespace function
} // namespace graphflow
