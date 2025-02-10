#pragma once

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
    // While stepActiveRelTableIDs is empty, using all relTableIDs in graph
    std::vector<common::table_id_set_t> stepActiveRelTableIDs;

    processor::NodeOffsetMaskMap* outputNodeMask = nullptr;

    GDSComputeState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute,
        std::vector<common::table_id_set_t> stepActiveRelTableIDs,
        processor::NodeOffsetMaskMap* outputNodeMask);
    virtual ~GDSComputeState();

    virtual void beginFrontierComputeBetweenTables(common::table_id_t currTableID,
        common::table_id_t nextTableID);

    common::table_id_set_t getActiveRelTableIDs(size_t index, graph::Graph* graph);
};

class KUZU_API GDSUtils {
public:
    static void runFrontiersUntilConvergence(processor::ExecutionContext* context,
        GDSComputeState& rjCompState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIteration, const std::string& propertyToScan = "");
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
