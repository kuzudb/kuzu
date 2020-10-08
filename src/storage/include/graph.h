#ifndef GRAPHFLOW_STORAGE_GRAPH_H_
#define GRAPHFLOW_STORAGE_GRAPH_H_

#include <vector>

#include "src/storage/include/catalog.h"

namespace graphflow {
namespace storage {

class Graph {

    std::unique_ptr<Catalog> catalog;
    std::unique_ptr<std::vector<uint64_t>> numNodesPerLabel, numRelsPerLabel;

public:
    Graph(std::unique_ptr<Catalog> catalog, std::unique_ptr<std::vector<uint64_t>> numNodesPerLabel,
        std::unique_ptr<std::vector<uint64_t>> numRelsPerLabel)
        : catalog(std::move(catalog)), numNodesPerLabel(std::move(numNodesPerLabel)),
          numRelsPerLabel(std::move(numRelsPerLabel)){};
};

} // namespace storage
} // namespace graphflow

#endif
