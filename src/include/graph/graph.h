#pragma once

#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>

#include "common/constants.h"
#include "common/copy_constructors.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include <span>

namespace kuzu {

namespace transaction {
class Transaction;
}

namespace graph {

struct RelTableIDInfo {
    common::table_id_t fromNodeTableID;
    common::table_id_t relTableID;
    common::table_id_t toNodeTableID;
};

class NbrScanState {
public:
    struct Chunk {
        friend class NbrScanState;

        // Any neighbour for which the given function returns false
        // will be omitted from future iterations
        // Used in GDSTask/EdgeCompute for updating the frontier
        template<class Func>
        void forEach(Func&& func) const {
            selVector.forEach([&](auto i) { func(nbrNodes[i], edges[i]); });
        }

        // Any neighbour for which the given function returns false
        // will be omitted from future iterations
        // Used in GDSTask/EdgeCompute for updating the frontier
        template<class T, class Func>
        void forEach(Func&& func) const {
            KU_ASSERT(propertyVector);
            selVector.forEach(
                [&](auto i) { func(nbrNodes[i], edges[i], propertyVector->getValue<T>(i)); });
        }

        uint64_t size() const { return selVector.getSelSize(); }

    private:
        Chunk(std::span<const common::nodeID_t> nbrNodes, std::span<const common::relID_t> edges,
            common::SelectionVector& selVector, const common::ValueVector* propertyVector)
            : nbrNodes{nbrNodes}, edges{edges}, selVector{selVector},
              propertyVector{propertyVector} {
            KU_ASSERT(nbrNodes.size() == common::DEFAULT_VECTOR_CAPACITY);
            KU_ASSERT(edges.size() == common::DEFAULT_VECTOR_CAPACITY);
        }

    private:
        std::span<const common::nodeID_t> nbrNodes;
        std::span<const common::relID_t> edges;
        // this reference can be modified, but the underlying data will be reset the next time next
        // is called
        common::SelectionVector& selVector;
        const common::ValueVector* propertyVector;
    };

    virtual ~NbrScanState() = default;
    virtual Chunk getChunk() = 0;

    // Returns true if there are more values after the current batch
    virtual bool next() = 0;

protected:
    Chunk createChunk(std::span<const common::nodeID_t> nbrNodes,
        std::span<const common::relID_t> edges, common::SelectionVector& selVector,
        const common::ValueVector* propertyVector) {
        return Chunk{nbrNodes, edges, selVector, propertyVector};
    }
};

class VertexScanState {
public:
    struct Chunk {
        friend class VertexScanState;

        size_t size() const { return nodeIDs.size(); }
        std::span<const common::nodeID_t> getNodeIDs() const { return nodeIDs; }
        template<typename T>
        std::span<const T> getProperties(size_t propertyIndex) const {
            return std::span(reinterpret_cast<const T*>(propertyVectors[propertyIndex]->getData()),
                nodeIDs.size());
        }

    private:
        Chunk(std::span<const common::nodeID_t> nodeIDs,
            std::span<const std::shared_ptr<common::ValueVector>> propertyVectors)
            : nodeIDs{nodeIDs}, propertyVectors{propertyVectors} {
            KU_ASSERT(nodeIDs.size() <= common::DEFAULT_VECTOR_CAPACITY);
        }

    private:
        std::span<const common::nodeID_t> nodeIDs;
        std::span<const std::shared_ptr<common::ValueVector>> propertyVectors;
    };
    virtual Chunk getChunk() = 0;

    // Returns true if there are more values after the current batch
    virtual bool next() = 0;

    virtual ~VertexScanState() = default;

protected:
    Chunk createChunk(std::span<const common::nodeID_t> nodeIDs,
        std::span<const std::shared_ptr<common::ValueVector>> propertyVectors) const {
        return Chunk{nodeIDs, propertyVectors};
    }
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
    class EdgeIterator {
    public:
        explicit constexpr EdgeIterator(NbrScanState* scanState) : scanState{scanState} {}
        DEFAULT_BOTH_MOVE(EdgeIterator);
        EdgeIterator(const EdgeIterator& other) = default;
        EdgeIterator() : scanState{nullptr} {}
        using difference_type = std::ptrdiff_t;
        using value_type = NbrScanState::Chunk;

        value_type operator*() const { return scanState->getChunk(); }
        EdgeIterator& operator++() {
            if (!scanState->next()) {
                scanState = nullptr;
            }
            return *this;
        }
        void operator++(int) { ++*this; }
        bool operator==(const EdgeIterator& other) const {
            // Only needed for comparing to the end, so they are equal if and only if both are null
            return scanState == nullptr && other.scanState == nullptr;
        }
        // Counts and consumes the iterator
        uint64_t count() {
            // TODO(bmwinger): avoid scanning if all that's necessary is to count the results
            uint64_t result = 0;
            do {
                result += scanState->getChunk().size();
            } while (scanState->next());
            return result;
        }

        std::vector<common::nodeID_t> collectNbrNodes() {
            std::vector<common::nodeID_t> nbrNodes;
            for (const auto chunk : *this) {
                nbrNodes.reserve(nbrNodes.size() + chunk.size());
                chunk.forEach(
                    [&](auto nbrNodeID, auto /*edgeID*/) { nbrNodes.push_back(nbrNodeID); });
            }
            return nbrNodes;
        }

        EdgeIterator& begin() noexcept { return *this; }
        static constexpr EdgeIterator end() noexcept { return EdgeIterator(nullptr); }

    private:
        NbrScanState* scanState;
    };
    static_assert(std::input_iterator<EdgeIterator>);

    Graph() = default;
    virtual ~Graph() = default;

    // Get id for all node tables.
    virtual std::vector<common::table_id_t> getNodeTableIDs() = 0;
    // Get id for all relationship tables.
    virtual std::vector<common::table_id_t> getRelTableIDs() = 0;

    virtual common::table_id_map_t<common::offset_t> getNumNodesMap(
        transaction::Transaction* transaction) = 0;

    // Get num rows for all node tables.
    virtual common::offset_t getNumNodes(transaction::Transaction* transcation) = 0;
    // Get num rows for given node table.
    virtual common::offset_t getNumNodes(transaction::Transaction* transcation,
        common::table_id_t id) = 0;

    // Get all possible "forward" (fromNodeTableID, relTableID, toNodeTableID) combinations.
    virtual std::vector<RelTableIDInfo> getRelTableIDInfos() = 0;

    // Prepares scan on the specified relationship table (works for backwards and forwards scans)
    virtual std::unique_ptr<NbrScanState> prepareScan(common::table_id_t relTableID,
        std::optional<common::idx_t> edgePropertyIndex = std::nullopt) = 0;
    // Prepares scan on all connected relationship tables using forward adjList.
    virtual std::unique_ptr<NbrScanState> prepareMultiTableScanFwd(
        std::span<common::table_id_t> nodeTableIDs) = 0;

    // scanFwd an scanBwd scan a single source node under the assumption that many nodes in the same
    // group will be scanned at once.

    // Get dst nodeIDs for given src nodeID using forward adjList.
    virtual EdgeIterator scanFwd(common::nodeID_t nodeID, NbrScanState& state) = 0;

    // We don't use scanBwd currently. I'm adding them because they are the mirroring to scanFwd.
    // Also, algorithm may only need adjList index in single direction so we should make double
    // indexing optional.

    // Prepares scan on all connected relationship tables using backward adjList.
    virtual std::unique_ptr<NbrScanState> prepareMultiTableScanBwd(
        std::span<common::table_id_t> nodeTableIDs) = 0;

    // Get dst nodeIDs for given src nodeID tables using backward adjList.
    virtual EdgeIterator scanBwd(common::nodeID_t nodeID, NbrScanState& state) = 0;

    class VertexIterator {
    public:
        explicit constexpr VertexIterator(VertexScanState* scanState) : scanState{scanState} {}
        DEFAULT_BOTH_MOVE(VertexIterator);
        VertexIterator(const VertexIterator& other) = default;
        VertexIterator() : scanState{nullptr} {}
        using difference_type = std::ptrdiff_t;
        using value_type = VertexScanState::Chunk;

        value_type operator*() const { return scanState->getChunk(); }
        VertexIterator& operator++() {
            if (!scanState->next()) {
                scanState = nullptr;
            }
            return *this;
        }
        void operator++(int) { ++*this; }
        bool operator==(const VertexIterator& other) const {
            // Only needed for comparing to the end, so they are equal if and only if both are null
            return scanState == nullptr && other.scanState == nullptr;
        }

        VertexIterator& begin() noexcept { return *this; }
        static constexpr VertexIterator end() noexcept { return VertexIterator(nullptr); }

    private:
        VertexScanState* scanState;
    };
    static_assert(std::input_iterator<EdgeIterator>);

    virtual std::unique_ptr<VertexScanState> prepareVertexScan(common::table_id_t tableID,
        const std::vector<std::string>& propertiesToScan) = 0;
    virtual VertexIterator scanVertices(common::offset_t startNodeOffset,
        common::offset_t endNodeOffsetExclusive, VertexScanState& scanState) = 0;
};

} // namespace graph
} // namespace kuzu
