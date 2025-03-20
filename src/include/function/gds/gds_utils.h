#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/extend_direction.h"
#include "gds_state.h"

namespace kuzu {
namespace function {

class KUZU_API GDSUtils {
public:
    static void runFrontiersUntilConvergence(processor::ExecutionContext* context,
        GDSComputeState& compState, graph::Graph* graph, common::ExtendDirection extendDirection,
        uint64_t maxIteration);
    // Run edge compute with output node mask for early termination
    static void runFrontiersUntilConvergence(processor::ExecutionContext* context,
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
    static void runVertexComputeSparse(SparseFrontier& sparseFrontier, graph::Graph* graph,
        VertexCompute& vc);
};

} // namespace function
} // namespace kuzu
