#pragma once

#include <bitset>
#include <cmath>

#include "common/data_chunk/data_chunk.h"
#include "processor/operator/base_partitioner_shared_state.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/local_cached_column.h"

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

struct EmbeddingHandle;

// This class is responsible for managing the lifetimes of retrieved embeddings
struct GetEmbeddingsScanState {
    GetEmbeddingsScanState() = default;
    virtual ~GetEmbeddingsScanState() = default;
    DELETE_BOTH_COPY(GetEmbeddingsScanState);
    DEFAULT_BOTH_MOVE(GetEmbeddingsScanState);

    virtual void* getEmbeddingPtr(const EmbeddingHandle& handle) = 0;

    // addEmbedding() is called when an embedding handle is allocated
    virtual void addEmbedding(const EmbeddingHandle& handle) = 0;

    // reclaimEmbedding() is called when an embedding handle does out of scope
    // and should reclaim any resources allocated for the embedding
    virtual void reclaimEmbedding(const EmbeddingHandle& handle) = 0;

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
};

struct EmbeddingHandle {
    explicit EmbeddingHandle(common::offset_t offset,
        GetEmbeddingsScanState* lifetimeManager = nullptr);
    ~EmbeddingHandle();
    DELETE_BOTH_COPY(EmbeddingHandle);
    EmbeddingHandle(EmbeddingHandle&&) noexcept;
    EmbeddingHandle& operator=(EmbeddingHandle&&) noexcept;

    void* getPtr() const { return isNull() ? nullptr : lifetimeManager->getEmbeddingPtr(*this); }
    bool isNull() const { return offsetInData == common::INVALID_OFFSET; }
    static EmbeddingHandle createNullHandle() { return EmbeddingHandle{common::INVALID_OFFSET}; }

    common::offset_t offsetInData;
    GetEmbeddingsScanState* lifetimeManager;
};

class HNSWIndexEmbeddings {
public:
    virtual ~HNSWIndexEmbeddings() = default;
    explicit HNSWIndexEmbeddings(common::ArrayTypeInfo typeInfo) : info{std::move(typeInfo)} {}

    virtual EmbeddingHandle getEmbedding(common::offset_t offset,
        GetEmbeddingsScanState& scanState) const = 0;
    virtual std::vector<EmbeddingHandle> getEmbeddings(std::span<const common::offset_t> offset,
        GetEmbeddingsScanState& scanState) const = 0;

    common::length_t getDimension() const { return info.getDimension(); }
    virtual std::unique_ptr<GetEmbeddingsScanState> constructScanState() const = 0;

protected:
    EmbeddingColumnInfo info;
};

class InMemEmbeddings final : public HNSWIndexEmbeddings {
public:
    InMemEmbeddings(transaction::Transaction* transaction, common::ArrayTypeInfo typeInfo,
        common::table_id_t tableID, common::column_id_t columnID);

    EmbeddingHandle getEmbedding(common::offset_t offset,
        GetEmbeddingsScanState& scanState) const override;
    std::vector<EmbeddingHandle> getEmbeddings(std::span<const common::offset_t> offset,
        GetEmbeddingsScanState& scanState) const override;

    std::unique_ptr<GetEmbeddingsScanState> constructScanState() const override;

private:
    storage::CachedColumn* data;
};

class OnDiskEmbeddingScanState final : public GetEmbeddingsScanState {
public:
    OnDiskEmbeddingScanState(const transaction::Transaction* transaction,
        storage::MemoryManager* mm, storage::NodeTable& nodeTable, common::column_id_t columnID,
        common::offset_t embeddingDim);

    void* getEmbeddingPtr(const EmbeddingHandle& handle) override;
    void addEmbedding(const EmbeddingHandle& handle) override;
    void reclaimEmbedding(const EmbeddingHandle& handle) override;

    storage::NodeTableScanState& getScanState() { return *scanState; }
    common::DataChunk& getScanChunk() { return scanChunk; }

private:
    std::unique_ptr<storage::NodeTableScanState> scanState;
    common::DataChunk scanChunk;

    // Used for managing used space in the output list data vector
    std::stack<common::offset_t> usedEmbeddingOffsets;
    std::bitset<common::DEFAULT_VECTOR_CAPACITY> allocatedOffsets;

    common::offset_t embeddingDim;
};

class OnDiskEmbeddings final : public HNSWIndexEmbeddings {
public:
    OnDiskEmbeddings(transaction::Transaction* transaction, storage::MemoryManager* mm,
        common::ArrayTypeInfo typeInfo, storage::NodeTable& nodeTable,
        common::column_id_t columnID);

    EmbeddingHandle getEmbedding(common::offset_t offset,
        GetEmbeddingsScanState& scanState) const override;
    std::vector<EmbeddingHandle> getEmbeddings(std::span<const common::offset_t> offset,
        GetEmbeddingsScanState& scanState) const override;
    std::unique_ptr<GetEmbeddingsScanState> constructScanState() const override;

private:
    transaction::Transaction* transaction;

    storage::MemoryManager* mm;
    storage::NodeTable& nodeTable;
    common::column_id_t columnID;
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
    { t.getInvalidOffset() } -> std::convertible_to<common::offset_t>;
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
        ReferenceType operator*() const {
            auto val = lookup.at(offset);
            return val == lookup.getInvalidOffset() ? common::INVALID_OFFSET : val;
        }
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

    // In the current implementation, race conditions can result in dstNode entries being skipped
    // during insertion. Skipped entries will be marked with this value
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
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setNodeOffsetToInvalid(common::offset_t csrOffset) {
        setNodeOffset(csrOffset, view->getInvalidOffset());
    }
    common::offset_t getNodeOffset(common::offset_t csrOffset) const {
        return view->getNodeOffsetAtomic(csrOffset);
    }

private:
    std::unique_ptr<storage::MemoryBuffer> buffer;
    std::unique_ptr<CompressedOffsetsView> view;
};

struct NodeToHNSWGraphOffsetMap {
    explicit NodeToHNSWGraphOffsetMap(common::offset_t numNodesInTable)
        : numNodesInGraph(numNodesInTable), numNodesInTable(numNodesInTable){};
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
    uint16_t incrementCSRLength(common::offset_t nodeOffset) {
        KU_ASSERT(nodeOffset < numNodes);
        while (true) {
            auto val = csrLengths[nodeOffset].load();
            if (val < maxDegree && csrLengths[nodeOffset].compare_exchange_strong(val, val + 1)) {
                return val;
            }
        }
    }
    void setDstNode(common::offset_t csrOffset, common::offset_t dstNode) {
        KU_ASSERT(csrOffset < numNodes * maxDegree);
        dstNodes.setNodeOffset(csrOffset, dstNode);
    }

    void setDstNodeInvalid(common::offset_t csrOffset) {
        dstNodes.setNodeOffsetToInvalid(csrOffset);
    }

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
};

} // namespace vector_extension
} // namespace kuzu
