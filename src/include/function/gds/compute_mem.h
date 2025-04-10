#pragma once

#include "graph/graph.h"

namespace kuzu {
namespace function {

class VertexCompute {
public:
    virtual void vertexCompute(const graph::VertexScanState::Chunk&) {}
    virtual ~VertexCompute() {}
};

} // namespace function
} // namespace kuzu
