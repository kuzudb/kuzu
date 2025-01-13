#pragma once

#include <optional>

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/extend_direction.h"
#include "common/types/types.h"

namespace kuzu {
namespace processor {
struct ExecutionContext;
class NodeOffsetMaskMap;
} // namespace processor

namespace graph {
class Graph;
}

namespace function {
struct FrontierTaskSharedState;
struct RJCompState;
class VertexCompute;
class EdgeCompute;
class FrontierPair;
class SparseFrontier;
struct VertexComputeTaskSharedState;
struct VertexComputeTaskInfo;

struct KUZU_API GDSComputeState {
    std::unique_ptr<function::FrontierPair> frontierPair;
    std::unique_ptr<function::EdgeCompute> edgeCompute;

    processor::NodeOffsetMaskMap* outputNodeMask = nullptr;

    GDSComputeState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute,
        processor::NodeOffsetMaskMap* outputNodeMask);
    virtual ~GDSComputeState();

    virtual void beginFrontierComputeBetweenTables(common::table_id_t currTableID,
        common::table_id_t nextTableID);
};

class KUZU_API GDSUtils {
public:
    static void scheduleFrontierTask(catalog::TableCatalogEntry* fromEntry,
        catalog::TableCatalogEntry* toEntry, catalog::TableCatalogEntry* relEntry,
        graph::Graph* graph, common::ExtendDirection extendDirection, GDSComputeState& rjCompState,
        processor::ExecutionContext* context, std::optional<uint64_t> numThreads = std::nullopt,
        const std::string& propertyToScan = "");
    static void runFrontiersUntilConvergence(processor::ExecutionContext* context,
        GDSComputeState& rjCompState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIters);
    // Run vertex compute without property scan
    static void runVertexCompute(processor::ExecutionContext* context, graph::Graph* graph,
        VertexCompute& vc);
    // Run vertex compute with property scan
    static void runVertexCompute(processor::ExecutionContext* context, graph::Graph* graph,
        VertexCompute& vc, std::vector<std::string> propertiesToScan);
    // Run vertex compute on specific table with property scan
    static void runVertexCompute(processor::ExecutionContext* context, graph::Graph* graph,
        VertexCompute& vc, catalog::TableCatalogEntry* entry,
        std::vector<std::string> propertiesToScan);
    static void runVertexComputeSparse(SparseFrontier& sparseFrontier, graph::Graph* graph,
        VertexCompute& vc);
};

} // namespace function
} // namespace kuzu
