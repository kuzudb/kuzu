#pragma once

#include "common/enums/extend_direction.h"

namespace kuzu {
namespace processor {
struct ExecutionContext;
}

namespace graph {
class Graph;
}

namespace function {
struct FrontierTaskSharedState;
struct RJCompState;
class VertexCompute;
class GDSUtils {
public:
    static void runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
        RJCompState& rjCompState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIters);
    static void runVertexComputeIteration(processor::ExecutionContext* executionContext,
        graph::Graph* graph, VertexCompute& vc);
};

} // namespace function
} // namespace kuzu
