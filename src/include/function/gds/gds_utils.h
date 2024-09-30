#pragma once

#include <memory>

namespace kuzu {
namespace processor {
struct ExecutionContext;
}

namespace graph {
class Graph;
}

namespace function {
class FrontierPair;
class EdgeCompute;
class FrontierTaskSharedState;
struct RJCompState;
class VertexCompute;
class GDSUtils {
public:
public:
    explicit GDSUtils();

    static void parallelizeFrontierCompute(processor::ExecutionContext* executionContext,
        std::shared_ptr<FrontierTaskSharedState> sharedState);
    static void runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
        RJCompState& rjCompState, graph::Graph* graph, uint64_t maxIters);
    static void runVertexComputeIteration(processor::ExecutionContext* executionContext,
        graph::Graph* graph, VertexCompute& vc);
};

} // namespace function
} // namespace kuzu
