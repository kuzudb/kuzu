#pragma once

#include <cstdint>
#include <iterator>
#include <memory>

#include "common/copy_constructors.h"
#include "common/data_chunk/sel_vector.h"
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
    struct Chunk {
        std::span<const common::nodeID_t> nbrNodes;
        std::span<const common::relID_t> edges;
        // this reference can be modified, but the underlying data will be reset the next time next
        // is called
        common::SelectionVector& selVector;
    };
    virtual ~GraphScanState() = default;
    virtual Chunk getChunk() = 0;

    // Returns true if there are more values after the current batch
    virtual bool next() = 0;
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
    class Iterator {
    public:
        explicit constexpr Iterator(GraphScanState* scanState) : scanState{scanState} {}
        DEFAULT_BOTH_MOVE(Iterator);
        Iterator(const Iterator& other) = default;
        Iterator() : scanState{nullptr} {}
        using iterator_category = std::input_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = GraphScanState::Chunk;

        value_type operator*() const { return scanState->getChunk(); }
        Iterator& operator++() {
            if (!scanState->next()) {
                scanState = nullptr;
            }
            return *this;
        }
        void operator++(int) { ++*this; }
        bool operator==(const Iterator& other) const {
            // Only needed for comparing to the end, so they are equal if and only if both are null
            return scanState == nullptr && other.scanState == nullptr;
        }
        // Counts and consumes the iterator
        uint64_t count() {
            // TODO(bmwinger): avoid scanning if all that's necessary is to count the results
            uint64_t result = 0;
            do {
                result += scanState->getChunk().selVector.getSelSize();
            } while (scanState->next());
            return result;
        }

        std::vector<common::nodeID_t> collectNbrNodes() {
            std::vector<common::nodeID_t> nbrNodes;
            // Old versions of apple clang have a bug which prevents capture bindings from being
            // captured by the lambda.
            // for (const auto [nbrNodes, edges, mask] : *this) // doesn't work here
            for (const auto chunk : *this) {
                nbrNodes.reserve(nbrNodes.size() + chunk.selVector.getSelSize());
                chunk.selVector.forEach([&](auto i) { nbrNodes.push_back(chunk.nbrNodes[i]); });
            }
            return nbrNodes;
        }

        Iterator& begin() noexcept { return *this; }
        static constexpr Iterator end() noexcept { return Iterator(nullptr); }

    private:
        GraphScanState* scanState;
    };
    static_assert(std::input_iterator<Iterator>);

    Graph() = default;
    virtual ~Graph() = default;

    // Get id for all node tables.
    virtual std::vector<common::table_id_t> getNodeTableIDs() = 0;
    // Get id for all relationship tables.
    virtual std::vector<common::table_id_t> getRelTableIDs() = 0;

    virtual std::unordered_map<common::table_id_t, uint64_t> getNodeTableIDAndNumNodes() = 0;

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
    virtual Iterator scanFwd(common::nodeID_t nodeID, GraphScanState& state) = 0;

    // We don't use scanBwd currently. I'm adding them because they are the mirroring to scanFwd.
    // Also, algorithm may only need adjList index in single direction so we should make double
    // indexing optional.

    // Prepares scan on all connected relationship tables using backward adjList.
    virtual std::unique_ptr<GraphScanState> prepareMultiTableScanBwd(
        std::span<common::table_id_t> nodeTableIDs) = 0;

    // Get dst nodeIDs for given src nodeID tables using backward adjList.
    virtual Iterator scanBwd(common::nodeID_t nodeID, GraphScanState& state) = 0;
};

} // namespace graph
} // namespace kuzu
