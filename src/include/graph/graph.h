#pragma once

#include "common/types/internal_id_t.h"

namespace kuzu {
namespace graph {

class Graph {
public:
    Graph() = default;
    virtual ~Graph() = default;

    virtual common::table_id_t getNodeTableID() = 0;

    virtual common::offset_t getNumNodes() = 0;

    virtual common::offset_t getNumEdges() = 0;

    virtual std::vector<common::nodeID_t> getNbrs(common::offset_t offset) = 0;
};

} // namespace graph
} // namespace kuzu
