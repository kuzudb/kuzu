#pragma once

#include <cmath>

#include "common/data_chunk/data_chunk.h"
#include "processor/operator/base_partitioner_shared_state.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/local_cached_column.h"
#include "storage/table/column_chunk_data.h"

namespace kuzu {
namespace storage {
struct NodeTableScanState;
}
namespace vector_extension {

struct EmbeddingColumnInfo {
    explicit EmbeddingColumnInfo(common::ArrayTypeInfo typeInfo) : typeInfo{std::move(typeInfo)} {}

    common::length_t getDimension() const { return typeInfo.getNumElements(); }

    common::ArrayTypeInfo typeInfo;
};

struct VectorEmbedding {
    explicit VectorEmbedding(void* embedding,
        std::unique_ptr<common::DataChunk> embeddingChunk = nullptr)
        : embedding(embedding), embeddingChunk(std::move(embeddingChunk)) {}

    bool isNull() const { return embedding == nullptr; }

    void* embedding;
    // This field should not be used directly
    // It is just used to maintain ownership of the memory pointed to by the embedding field if
    // necessary
    std::unique_ptr<common::DataChunk> embeddingChunk;
};

struct GetEmbeddingsLocalState {
    virtual ~GetEmbeddingsLocalState() = default;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
};

class CreateHNSWIndexEmbeddings {
public:
    virtual ~CreateHNSWIndexEmbeddings() = default;
    explicit CreateHNSWIndexEmbeddings(common::ArrayTypeInfo typeInfo)
        : info{std::move(typeInfo)} {}
    virtual VectorEmbedding getEmbedding(common::offset_t offset,
        GetEmbeddingsLocalState& localState) const = 0;
    virtual bool isNull(common::offset_t offset, GetEmbeddingsLocalState& localState) const = 0;
    common::length_t getDimension() const { return info.getDimension(); }
    virtual std::unique_ptr<GetEmbeddingsLocalState> constructLocalState() {
        return std::make_unique<GetEmbeddingsLocalState>();
    };

protected:
    EmbeddingColumnInfo info;
};

class InMemEmbeddings final : public CreateHNSWIndexEmbeddings {
public:
    InMemEmbeddings(transaction::Transaction* transaction, common::ArrayTypeInfo typeInfo,
        common::table_id_t tableID, common::column_id_t columnID);

    VectorEmbedding getEmbedding(common::offset_t offset,
        GetEmbeddingsLocalState& localState) const override;
    bool isNull(common::offset_t offset, GetEmbeddingsLocalState& localState) const override;

private:
    storage::CachedColumn* data;
};

struct OnDiskEmbeddingScanState {
    OnDiskEmbeddingScanState(const transaction::Transaction* transaction,
        storage::MemoryManager* mm, storage::NodeTable& nodeTable, common::column_id_t columnID);

    static void initScanState(const transaction::Transaction* transaction,
        storage::MemoryManager* mm, storage::NodeTable& nodeTable, common::column_id_t columnID,
        common::DataChunk& scanChunk, std::unique_ptr<storage::NodeTableScanState>& scanState);

    common::DataChunk getAndResetDataChunk();

    std::unique_ptr<storage::NodeTableScanState> scanState;
    common::DataChunk scanChunk;
};

class OnDiskEmbeddings final {
public:
    OnDiskEmbeddings(common::ArrayTypeInfo typeInfo, storage::NodeTable& nodeTable)
        : info{std::move(typeInfo)}, nodeTable{nodeTable} {}

    void* getEmbedding(transaction::Transaction* transaction,
        storage::NodeTableScanState& scanState, common::offset_t offset) const;

    std::vector<void*> getEmbeddings(transaction::Transaction* transaction,
        storage::NodeTableScanState& scanState, const std::vector<common::offset_t>& offsets) const;

    common::length_t getDimension() const { return info.getDimension(); }

private:
    EmbeddingColumnInfo info;
    storage::NodeTable& nodeTable;
};

class OnDiskCreateHNSWIndexEmbeddings : public CreateHNSWIndexEmbeddings {
public:
    OnDiskCreateHNSWIndexEmbeddings(transaction::Transaction* transaction,
        storage::MemoryManager* mm, common::ArrayTypeInfo typeInfo, storage::NodeTable& nodeTable,
        common::column_id_t columnID);

    VectorEmbedding getEmbedding(common::offset_t offset,
        GetEmbeddingsLocalState& localState) const override;
    bool isNull(common::offset_t offset, GetEmbeddingsLocalState& localState) const override;
    std::unique_ptr<GetEmbeddingsLocalState> constructLocalState() override;

private:
    OnDiskEmbeddings embeddings;
    transaction::Transaction* transaction;

    std::vector<common::LogicalType> types;
    storage::MemoryManager* mm;
    storage::NodeTable& nodeTable;
    common::column_id_t columnID;
    std::shared_ptr<common::DataChunkState> scanChunkState;
};

struct NodeWithDistance {
    common::offset_t nodeOffset;
    double_t distance;

    NodeWithDistance()
        : nodeOffset{common::INVALID_OFFSET}, distance{std::numeric_limits<double_t>::max()} {}
    NodeWithDistance(common::offset_t nodeOffset, double_t distance)
        : nodeOffset{nodeOffset}, distance{distance} {}
};

template<typename T, typename ReferenceType>
concept OffsetRangeLookup = requires(T t, common::offset_t offset) {
    { t.at(offset) } -> std::convertible_to<ReferenceType>;
};

/**
 * @brief Utility class that allows iteration using range-based for loops. Instantiations of this
 * class just need to provide a class that performs the lookup based on the current offset
 *
 * @tparam ReferenceType the return type of the lookup
 * @tparam Lookup the class that contains the lookup operation at(offset)
 */
template<typename ReferenceType, OffsetRangeLookup<ReferenceType> Lookup>
struct CompressedOffsetSpan {
    struct Iterator {
        bool operator==(const Iterator& o) const { return !(*this != o); }
        bool operator!=(const Iterator& o) const { return offset != o.offset; }
        ReferenceType operator*() const { return lookup.at(offset); }
        void operator++() { ++offset; }

        const Lookup& lookup;
        common::offset_t offset;
    };

    Iterator begin() const { return Iterator{lookup, startOffset}; }
    Iterator end() const { return Iterator{lookup, endOffset}; }

    const Lookup& lookup;
    common::offset_t startOffset;
    common::offset_t endOffset;
};

struct CompressedOffsetsView {
    virtual ~CompressedOffsetsView() = default;

    virtual common::offset_t getNodeOffsetAtomic(common::offset_t csrOffset) const = 0;
    virtual void setNodeOffsetAtomic(common::offset_t csrOffset, common::offset_t nodeID) = 0;
    common::offset_t at(common::offset_t offset) const { return getNodeOffsetAtomic(offset); };
    virtual common::offset_t getInvalidOffset() const = 0;
};

using compressed_offsets_t = CompressedOffsetSpan<common::offset_t, const CompressedOffsetsView&>;
class CompressedNodeOffsetBuffer {
public:
    CompressedNodeOffsetBuffer(storage::MemoryManager* mm, common::offset_t numNodes,
        common::length_t maxDegree);

    compressed_offsets_t getNeighbors(common::offset_t nodeOffset, common::offset_t maxDegree,
        common::offset_t numNbrs) const;

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setNodeOffset(common::offset_t csrOffset, common::offset_t nodeOffset) {
        view->setNodeOffsetAtomic(csrOffset, nodeOffset);
    }
    common::offset_t getNodeOffset(common::offset_t csrOffset) const {
        return view->getNodeOffsetAtomic(csrOffset);
    }

    common::offset_t getInvalidOffset() const { return view->getInvalidOffset(); }

private:
    std::unique_ptr<storage::MemoryBuffer> buffer;
    std::unique_ptr<CompressedOffsetsView> view;
};

struct NodeToHNSWGraphOffsetMap {
    explicit NodeToHNSWGraphOffsetMap(common::offset_t numNodesInTable)
        : numNodesInGraph(numNodesInTable), numNodesInTable(numNodesInTable) {};
    NodeToHNSWGraphOffsetMap(common::offset_t numNodesInTable,
        const common::NullMask* selectedNodes);

    common::offset_t getNumNodesInGraph() const { return numNodesInGraph; }
    common::offset_t nodeToGraphOffset(common::offset_t nodeOffset, bool exactMatch = true) const;
    common::offset_t graphToNodeOffset(common::offset_t graphOffset) const;
    bool isTrivialMapping() const { return graphToNodeMap == nullptr; }

    common::offset_t numNodesInGraph;
    common::offset_t numNodesInTable;
    std::unique_ptr<common::offset_t[]> graphToNodeMap;
};

class InMemHNSWGraph {
public:
    InMemHNSWGraph(storage::MemoryManager* mm, common::offset_t numNodes,
        common::length_t maxDegree);

    compressed_offsets_t getNeighbors(common::offset_t nodeOffset) const {
        const auto numNbrs = getCSRLength(nodeOffset);
        KU_ASSERT(numNbrs <= maxDegree);
        KU_ASSERT(nodeOffset < numNodes);
        return dstNodes.getNeighbors(nodeOffset, maxDegree, numNbrs);
    }

    common::length_t getMaxDegree() const { return maxDegree; }

    uint16_t getCSRLength(common::offset_t nodeOffset) const {
        KU_ASSERT(nodeOffset < numNodes);
        const auto val = csrLengths[nodeOffset].load();
        KU_ASSERT(val <= maxDegree);
        return val;
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setCSRLength(common::offset_t nodeOffset, uint16_t length) {
        KU_ASSERT(nodeOffset < numNodes);
        KU_ASSERT(length <= maxDegree);
        csrLengths[nodeOffset].store(length);
    }
    // Note: when the incremented csr length hits maxDegree, this function will block until there is
    // a shrink happening.
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    uint16_t incrementCSRLength(common::offset_t nodeOffset) {
        KU_ASSERT(nodeOffset < numNodes);
        while (true) {
            auto val = csrLengths[nodeOffset].load();
            if (val < maxDegree && csrLengths[nodeOffset].compare_exchange_strong(val, val + 1)) {
                return val;
            }
        }
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setDstNode(common::offset_t csrOffset, common::offset_t dstNode) {
        KU_ASSERT(csrOffset < numNodes * maxDegree);
        dstNodes.setNodeOffset(csrOffset, dstNode);
    }

    // In the current implementation, race conditions can result in dstNode entries being skipped
    // during insertion. Skipped entries will be marked with this value
    common::offset_t getInvalidOffset() const { return invalidOffset; }

private:
    void resetCSRLengthAndDstNodes();

    common::offset_t getDstNode(common::offset_t csrOffset) const {
        return dstNodes.getNodeOffset(csrOffset);
    }

private:
    common::offset_t numNodes;
    std::unique_ptr<storage::MemoryBuffer> csrLengthBuffer;
    std::atomic<uint16_t>* csrLengths;
    CompressedNodeOffsetBuffer dstNodes;
    // Max allowed degree of a node in the graph before shrinking.
    common::length_t maxDegree;
    common::offset_t invalidOffset;
};

} // namespace vector_extension
} // namespace kuzu
