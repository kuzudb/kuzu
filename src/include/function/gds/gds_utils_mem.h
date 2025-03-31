#pragma once

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/extend_direction.h"
#include "gds_state.h"

namespace kuzu {
namespace function {

class GDSUtilsInMemory {
public:
    static void runVertexCompute(processor::ExecutionContext* context, graph::Graph* graph,
        VertexCompute& vc);
};

} // namespace function
} // namespace kuzu
