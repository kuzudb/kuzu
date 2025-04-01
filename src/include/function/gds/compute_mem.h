#pragma once

#include "common/mask.h"
#include "common/types/types.h"
#include "graph/graph.h"

namespace kuzu {
namespace function {

class VertexCompute {
public:
    virtual void vertexCompute(const graph::VertexScanState::Chunk&) {}
};

} // namespace function
} // namespace kuzu
