#pragma once

#include <memory>

#include "common/types/types.h"
#include <span>

namespace kuzu {
namespace graph {

struct RelTableIDInfo {
    common::table_id_t fromNodeTableID;
    common::table_id_t relTableID;
    common::table_id_t toNodeTableID;
};

class GraphScanState {
public:
    virtual ~GraphScanState() = default;
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

    // Prepares scan on the specified relationship table (works for backwards and forwards scans)
    virtual std::unique_ptr<GraphScanState> prepareScan(common::table_id_t relTableID) = 0;
    // Prepares scan on all connected relationship tables using forward adjList.
    virtual std::unique_ptr<GraphScanState> prepareMultiTableScanFwd(
        std::span<common::table_id_t> nodeTableIDs) = 0;

    // scanFwd an scanBwd scan a single source node under the assumption that many nodes in the same
    // group will be scanned at once.

    // Get dst nodeIDs for given src nodeID using forward adjList.
    virtual std::vector<common::nodeID_t> scanFwd(common::nodeID_t nodeID,
        GraphScanState& state) = 0;

    // Scans multiple nodeIDs in random mode, which is optimized for small lookups and does minimal
    // caching of CSR headers.
    virtual std::vector<common::nodeID_t> scanFwdRandom(common::nodeID_t nodeID,
        GraphScanState& state) = 0;

    // We don't use scanBwd currently. I'm adding them because they are the mirroring to scanFwd.
    // Also, algorithm may only need adjList index in single direction so we should make double
    // indexing optional.

    // Prepares scan on all connected relationship tables using backward adjList.
    virtual std::unique_ptr<GraphScanState> prepareMultiTableScanBwd(
        std::span<common::table_id_t> nodeTableIDs) = 0;

    // Get dst nodeIDs for given src nodeID tables using backward adjList.
    virtual std::vector<common::nodeID_t> scanBwd(common::nodeID_t nodeID,
        GraphScanState& state) = 0;
    virtual std::vector<common::nodeID_t> scanBwdRandom(common::nodeID_t nodeID,
        GraphScanState& state) = 0;
};

} // namespace graph
} // namespace kuzu
