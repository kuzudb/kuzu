#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace graph {

class Graph {
public:
    explicit Graph(main::ClientContext* context) : context{context} {}
    virtual ~Graph() = default;

    virtual common::offset_t getNumNodes() = 0;

    virtual common::offset_t getNumEdges() = 0;

protected:
    main::ClientContext* context;
};

} // namespace graph
} // namespace kuzu
