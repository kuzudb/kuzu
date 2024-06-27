#pragma once

namespace kuzu {
namespace processor {
struct ExecutionContext;
}

namespace graph {
class Graph;
}

namespace function {
class Frontiers;
class FrontierUpdateFn;
class FrontierTaskSharedState;


class GDSUtils {
public:
    explicit GDSUtils();

    static void parallelizeFrontierVertexUpdate(processor::ExecutionContext* executionContext,
        std::shared_ptr<FrontierTaskSharedState> sharedState);
//        Frontiers& frontiers, graph::Graph* graph, FrontierUpdateFn& vu);
    static void runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
        Frontiers& frontiers, graph::Graph* graph, FrontierUpdateFn& vu, uint64_t maxIters);
};

} // namespace function
} // namespace kuzu