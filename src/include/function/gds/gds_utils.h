#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/extend_direction.h"
#include "gds_state.h"

namespace kuzu {
namespace function {

class KUZU_API GDSUtils {
public:
    // Run edge compute for graph algorithms
    static void runAlgorithmEdgeCompute(processor::ExecutionContext* context,
        GDSComputeState& compState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIteration);
    // Run edge compute for full text search
    static void runFTSEdgeCompute(processor::ExecutionContext* context, GDSComputeState& compState,
        graph::Graph* graph, common::ExtendDirection extendDirection,
        const std::string& propertyToScan);
    // Run edge compute for recursive join.
    static void runRecursiveJoinEdgeCompute(processor::ExecutionContext* context,
        GDSComputeState& compState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIteration, common::NodeOffsetMaskMap* outputNodeMask,
        const std::string& propertyToScan = "");

    // Run vertex compute without property scan
    static void runVertexCompute(processor::ExecutionContext* context, GDSDensityState densityState,
        graph::Graph* graph, VertexCompute& vc);
    // Run vertex compute with property scan
    static void runVertexCompute(processor::ExecutionContext* context, GDSDensityState densityState,
        graph::Graph* graph, VertexCompute& vc, std::vector<std::string> propertiesToScan);
    // Run vertex compute on specific table with property scan
    static void runVertexCompute(processor::ExecutionContext* context, GDSDensityState densityState,
        graph::Graph* graph, VertexCompute& vc, catalog::TableCatalogEntry* entry,
        std::vector<std::string> propertiesToScan);
};

} // namespace function
} // namespace kuzu
