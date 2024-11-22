#pragma once

#include <optional>

#include "common/enums/extend_direction.h"
#include "common/types/types.h"

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
class EdgeCompute;
class FrontierPair;
struct VertexComputeTaskSharedState;
struct VertexComputeTaskInfo;

struct KUZU_API GDSComputeState {
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;

    GDSComputeState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute);
    ~GDSComputeState();
};

class KUZU_API GDSUtils {
public:
    static void scheduleFrontierTask(common::table_id_t relTableID, graph::Graph* graph,
        common::ExtendDirection extendDirection, GDSComputeState& rjCompState,
        processor::ExecutionContext* context, std::optional<uint64_t> numThreads = std::nullopt,
        std::optional<common::idx_t> edgePropertyIdx = std::nullopt);
    static void runFrontiersUntilConvergence(processor::ExecutionContext* executionContext,
        RJCompState& rjCompState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIters);
    static void runVertexComputeOnTable(common::table_id_t tableID, graph::Graph* graph,
        std::shared_ptr<VertexComputeTaskSharedState> sharedState,
        const VertexComputeTaskInfo& info, processor::ExecutionContext& context);
    static void runVertexComputeIteration(processor::ExecutionContext* executionContext,
        graph::Graph* graph, VertexCompute& vc, std::vector<std::string> propertiesToScan = {});
};

} // namespace function
} // namespace kuzu
