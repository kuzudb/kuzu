#ifndef GRAPHFLOW_STORAGE_GRAPH_H_
#define GRAPHFLOW_STORAGE_GRAPH_H_

#include <vector>

#include "src/storage/include/catalog.h"

namespace graphflow {
namespace common {

class GraphLoader;

} // namespace common
} // namespace graphflow

namespace graphflow {
namespace storage {

class Graph {

    friend class graphflow::common::GraphLoader;

private:
    Graph(){};

private:
    std::unique_ptr<Catalog> catalog;
    std::vector<uint64_t> numNodesPerLabel, numRelsPerLabel;
};

} // namespace storage
} // namespace graphflow

#endif
