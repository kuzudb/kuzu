#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/extend_direction.h"
#include "gds_state.h"

namespace kuzu {
namespace function {

class KUZU_API GDSUtils {
public:
    static void runAlgorithmEdgeCompute(processor::ExecutionContext* context,
        GDSComputeState& compState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIteration);

    static void runFTSEdgeCompute(processor::ExecutionContext* context, GDSComputeState& compState,
        graph::Graph* graph, common::ExtendDirection extendDirection,
        const std::string& propertyToScan);

    // Run edge compute for recursive join. TODO continue comment
    static void runRecursiveJoinEdgeCompute(processor::ExecutionContext* context,
        GDSComputeState& compState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIteration, common::NodeOffsetMaskMap* outputNodeMask,
        const std::string& propertyToScan = "");

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
    static void runVertexComputeSparse(graph::Graph* graph, VertexCompute& vc);
};

} // namespace function
} // namespace kuzu
