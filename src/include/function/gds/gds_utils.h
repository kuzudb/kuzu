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
class Frontiers;
class FrontierCompute;
class FrontierTaskSharedState;
struct RJCompState;
class GDSUtils {
public:
    explicit GDSUtils();

    static void parallelizeFrontierCompute(processor::ExecutionContext* executionContext,
        std::shared_ptr<FrontierTaskSharedState> sharedState);
    static void runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
        RJCompState& rjCompState, graph::Graph* graph, uint64_t maxIters);
    // TODO(Semih): Remove
    //    static void runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
//        Frontiers& frontiers, graph::Graph* graph, FrontierCompute& fc, uint64_t maxIters);
};

} // namespace function
} // namespace kuzu
