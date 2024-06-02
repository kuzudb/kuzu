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
    explicit GraphAlgorithm(std::shared_ptr<ParallelUtils> parallelUtils) : parallelUtils{std::move(parallelUtils)} {}
    virtual ~GraphAlgorithm() = default;
    virtual void compute(ExecutionContext* executionContext) = 0;

protected:
    std::shared_ptr<ParallelUtils> parallelUtils;
};

// CallFunction has the assumption that number of output is known before execution.
// This does NOT hold for all graph algorithms.
// So each algorithm need to decide its own shared state to control when to terminate.
struct DemoAlgorithm : public GraphAlgorithm {
public:
    explicit DemoAlgorithm(std::shared_ptr<ParallelUtils> parallelUtils) : GraphAlgorithm(parallelUtils) {}
    static constexpr const char* name = "DEMO_ALGORITHM";
    void compute(ExecutionContext* executionContext) override;
    static function::function_set getFunctionSet();
};

struct VariableLengthPath {
    static constexpr const char* name = "VARIABLE_LENGTH_PATH";

    static function::function_set getFunctionSet();
};

struct ShortestPath : public GraphAlgorithm {
public:
    explicit ShortestPath(std::shared_ptr<ParallelUtils> parallelUtils) : GraphAlgorithm(parallelUtils) {}
    static constexpr const char* name = "SHORTEST_PATH";
    void compute(ExecutionContext* executionContext) override;
    static function::function_set getFunctionSet();
};

struct PageRank {
    static constexpr const char* name = "DEMO_ALGO";

    static function::function_set getFunctionSet();
};

} // namespace graph
} // namespace kuzu
