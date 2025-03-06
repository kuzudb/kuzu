#pragma once

#include "processor/operator/partitioner.h"
#include "storage/index/hnsw_config.h"
#include "storage/local_cached_column.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu {
namespace storage {

struct EmbeddingTypeInfo {
    common::LogicalType elementType;
    common::length_t dimension;
};

class EmbeddingColumn {
public:
    explicit EmbeddingColumn(EmbeddingTypeInfo typeInfo) : typeInfo{std::move(typeInfo)} {}
    virtual ~EmbeddingColumn() = default;

    const EmbeddingTypeInfo& getTypeInfo() const { return typeInfo; }
    common::length_t getDimension() const { return typeInfo.dimension; }

protected:
    EmbeddingTypeInfo typeInfo;
};

class InMemEmbeddings final : public EmbeddingColumn {
public:
    InMemEmbeddings(transaction::Transaction* transaction, EmbeddingTypeInfo typeInfo,
        common::table_id_t tableID, common::column_id_t columnID);

    float* getEmbedding(common::offset_t offset) const;
    bool isNull(common::offset_t offset) const;

private:
    CachedColumn* data;
};

struct NodeTableScanState;
struct OnDiskEmbeddingScanState {
    common::DataChunk scanChunk;
    std::unique_ptr<NodeTableScanState> scanState;

    OnDiskEmbeddingScanState(const transaction::Transaction* transaction, MemoryManager* mm,
        NodeTable& nodeTable, common::column_id_t columnID);
};

class OnDiskEmbeddings final : public EmbeddingColumn {
public:
    OnDiskEmbeddings(EmbeddingTypeInfo typeInfo, NodeTable& nodeTable,
        common::column_id_t columnID);

    float* getEmbedding(transaction::Transaction* transaction, NodeTableScanState& scanState,
        common::offset_t offset) const;

private:
    NodeTable& nodeTable;
    common::column_id_t columnID;
};

struct NodeWithDistance {
    common::offset_t nodeOffset;
    double_t distance;

    NodeWithDistance(common::offset_t nodeOffset, double_t distance)
        : nodeOffset{nodeOffset}, distance{distance} {}
};

struct HNSWGraphInfo {
    common::offset_t numNodes;
    EmbeddingColumn* embeddings;
    DistFuncType distFunc;

    HNSWGraphInfo(common::offset_t numNodes, EmbeddingColumn* embeddings, DistFuncType distFunc)
        : numNodes{numNodes}, embeddings{embeddings}, distFunc{distFunc} {}
};

class InMemHNSWGraph {
public:
    using shrink_func_t =
        std::function<void(transaction::Transaction*, common::offset_t, common::length_t)>;

    InMemHNSWGraph(MemoryManager* mm, common::offset_t numNodes, common::length_t maxDegree)
        : numNodes{numNodes}, maxDegree{maxDegree} {
        csrLengthBuffer = mm->allocateBuffer(true, numNodes * sizeof(std::atomic<uint16_t>));
        csrLengths = reinterpret_cast<std::atomic<uint16_t>*>(csrLengthBuffer->getData());
        dstNodesBuffer =
            mm->allocateBuffer(false, numNodes * maxDegree * sizeof(std::atomic<common::offset_t>));
        dstNodes = reinterpret_cast<std::atomic<common::offset_t>*>(dstNodesBuffer->getData());
        resetCSRLengthAndDstNodes();
    }

    std::span<std::atomic<common::offset_t>> getNeighbors(common::offset_t nodeOffset) const {
        const auto numNbrs = getCSRLength(nodeOffset);
        return {&dstNodes[nodeOffset * maxDegree], numNbrs};
    }

    common::length_t getMaxDegree() const { return maxDegree; }

    uint16_t getCSRLength(common::offset_t nodeOffset) const {
        return csrLengths[nodeOffset].load(std::memory_order_relaxed);
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setCSRLength(common::offset_t nodeOffset, uint16_t length) {
        csrLengths[nodeOffset].store(length, std::memory_order_relaxed);
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    uint16_t incrementCSRLength(common::offset_t nodeOffset) {
        return csrLengths[nodeOffset].fetch_add(1, std::memory_order_relaxed);
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setDstNode(common::offset_t csrOffset, common::offset_t dstNode) {
        dstNodes[csrOffset].store(dstNode, std::memory_order_relaxed);
    }

    void finalize(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        const processor::PartitionerSharedState& partitionerSharedState);

private:
    void resetCSRLengthAndDstNodes();

    void finalizeNodeGroup(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        uint64_t numRels, common::table_id_t srcNodeTableID, common::table_id_t dstNodeTableID,
        common::table_id_t relTableID, InMemChunkedNodeGroupCollection& partition) const;

    common::offset_t getDstNode(common::offset_t csrOffset) const {
        return dstNodes[csrOffset].load(std::memory_order_relaxed);
    }

private:
    common::offset_t numNodes;
    std::unique_ptr<MemoryBuffer> csrLengthBuffer;
    std::unique_ptr<MemoryBuffer> dstNodesBuffer;
    std::atomic<uint16_t>* csrLengths;
    std::atomic<common::offset_t>* dstNodes;
    // Max allowed degree of a node in the graph before shrinking.
    common::length_t maxDegree;
};

} // namespace storage
} // namespace kuzu
