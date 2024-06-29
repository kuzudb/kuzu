#pragma once

#include <memory>

#include "common/types/internal_id_t.h"

namespace kuzu {
namespace graph {

struct RelTableIDInfo {
    common::table_id_t fromNodeTableID;
    common::table_id_t relTableID;
    common::table_id_t toNodeTableID;
};

/**
 * Graph interface to be use by GDS algorithms to get neighbors of nodes.
 *
 * Instances of Graph are not expected to be thread-safe. Therefore, if Graph is intended to be used
 * in a parallel manner, the user should first copy() an instance and give each thread a separate
 * copy. It is the responsibility of the implementing Graph class that the copy() is a lightweight
 * operation that does not copy large amounts of data between instances.
 */
class Graph {
public:
    Graph() = default;
    virtual ~Graph() = default;

    virtual std::unique_ptr<Graph> copy() = 0;
    // Get id for all node tables.
    virtual std::vector<common::table_id_t> getNodeTableIDs() = 0;
    // Get id for all relationship tables.
    virtual std::vector<common::table_id_t> getRelTableIDs() = 0;

    // Get num rows for all node tables.
    virtual common::offset_t getNumNodes() = 0;
    // Get num rows for given node table.
    virtual common::offset_t getNumNodes(common::table_id_t id) = 0;

    // Get all possible "forward" (fromNodeTableID, relTableID, toNodeTableID) combinations.
    virtual std::vector<RelTableIDInfo> getRelTableIDInfos() = 0;

    // Get dst nodeIDs for given src nodeID on all connected relationship tables using forward
    // adjList.
    virtual std::vector<common::nodeID_t> scanFwd(common::nodeID_t nodeID) = 0;
    // Get dst nodeIDs for given src nodeID on given relationship table using forward adjList.
    virtual std::vector<common::nodeID_t> scanFwd(common::nodeID_t nodeID,
        common::table_id_t relTableID) = 0;

    // We don't use scanBwd currently. I'm adding them because they are the mirroring to scanFwd.
    // Also, algorithm may only need adjList index in single direction so we should make double
    // indexing optional.

    // Get dst nodeIDs for given src nodeID on all connected relationship tables using backward
    // adjList.
    virtual std::vector<common::nodeID_t> scanBwd(common::nodeID_t nodeID) = 0;
    // Get dst nodeIDs for given src nodeID on given relationship table using backward adjList.
    virtual std::vector<common::nodeID_t> scanBwd(common::nodeID_t nodeID,
        common::table_id_t relTableID) = 0;
};

} // namespace graph
} // namespace kuzu
