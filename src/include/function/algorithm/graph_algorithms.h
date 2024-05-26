#pragma once

#include "function/function.h"
#include "function/table/call_functions.h"
#include "ife_morsel.h"
#include "parallel_utils.h"

namespace kuzu {
namespace graph {

struct ShortestPathAlgoSharedState;

struct GraphAlgorithm {
public:
    GraphAlgorithm() {}
    virtual ~GraphAlgorithm() = default;
    virtual void compute(Sink *sink, ExecutionContext* executionContext,
        std::shared_ptr<ParallelUtils> parallelUtils) = 0;
};

// CallFunction has the assumption that number of output is known before execution.
// This does NOT hold for all graph algorithms.
// So each algorithm need to decide its own shared state to control when to terminate.
struct DemoAlgorithm : public GraphAlgorithm {
public:
    DemoAlgorithm() {}
    static constexpr const char* name = "DEMO_ALGORITHM";
    void compute(Sink *sink, ExecutionContext* executionContext,
        std::shared_ptr<ParallelUtils> parallelUtils) override;
    static function::function_set getFunctionSet();
};

struct VariableLengthPath {
    static constexpr const char* name = "VARIABLE_LENGTH_PATH";

    static function::function_set getFunctionSet();
};

struct ShortestPath : public GraphAlgorithm {
public:
    static constexpr const char* name = "SHORTEST_PATH";
    ShortestPathAlgoSharedState* getSharedState(Sink *sink);
    void incrementTableFuncIdx(Sink *sink);
    void compute(Sink *sink, ExecutionContext* executionContext,
        std::shared_ptr<ParallelUtils> parallelUtils) override;
    static function::function_set getFunctionSet();
};

struct PageRank {
    static constexpr const char* name = "DEMO_ALGO";

    static function::function_set getFunctionSet();
};

} // namespace graph
} // namespace kuzu
