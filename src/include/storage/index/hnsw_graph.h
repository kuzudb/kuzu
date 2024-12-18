#pragma once

#include "hnsw_config.h"
#include "processor/operator/partitioner.h"
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

    virtual float* getEmebdding(common::offset_t offset,
        transaction::Transaction* transaction) const = 0;

    const EmbeddingTypeInfo& getTypeInfo() const { return typeInfo; }

    virtual void initialize(main::ClientContext*, NodeTable&, common::column_id_t) {}

protected:
    EmbeddingTypeInfo typeInfo;
};

// Cache of embeddings in the node table. Note that this can potentially reduce the scalability
// of the index, but improves the performance by avoiding random reads from disk. Though we need
// to benchmark to see the effect on different dimension sizes. Assume embeddings are not
// compressed, for FLOAT[1024], a single embedding is 4KB, which is a reasonable size to read
// from disk, while caching 10M embeddings of such in memory will cost 40GB of memory.
class InMemEmbeddings final : public EmbeddingColumn {
public:
    InMemEmbeddings(MemoryManager& mm, EmbeddingTypeInfo typeInfo, common::offset_t numNodes,
        const common::LogicalType& columnType);

    float* getEmebdding(common::offset_t offset,
        transaction::Transaction* transaction = nullptr) const override;
    ColumnChunkData& getData() const { return *data; }

    void initialize(main::ClientContext* context, NodeTable& table,
        common::column_id_t columnID) override;

private:
    std::unique_ptr<ColumnChunkData> data;
};

struct NodeTableScanState;
class OnDiskEmbeddings final : public EmbeddingColumn {
public:
    OnDiskEmbeddings(MemoryManager* mm, EmbeddingTypeInfo typeInfo, NodeTable& nodeTable,
        common::column_id_t columnID);

    float* getEmebdding(common::offset_t offset,
        transaction::Transaction* transaction) const override;

private:
    NodeTable& nodeTable;
    common::column_id_t columnID;

    // TODO(Guodong): Should move these to local state.
    // Cacheed data structures used to read from table.
    common::DataChunk propertyVectors;
    std::unique_ptr<common::ValueVector> nodeIDVector;
    std::unique_ptr<NodeTableScanState> scanState;
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

class HNSWGraph {
public:
    explicit HNSWGraph(const HNSWGraphInfo& info)
        : entryPoint{common::INVALID_OFFSET}, numNodes{info.numNodes}, embeddings{info.embeddings},
          distFunc{info.distFunc} {}
    virtual ~HNSWGraph() = default;

    virtual common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset) const = 0;
    virtual common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset, common::length_t numNbrs) const = 0;

    void setEntryPoint(common::offset_t entryPoint_) { entryPoint.store(entryPoint_); }

    common::offset_t getEntryPoint() const { return entryPoint.load(); }
    common::offset_t getNumNodes() const { return numNodes; }

    virtual void createDirectedEdge(transaction::Transaction*, common::offset_t, common::offset_t) {
        KU_UNREACHABLE;
    }
    virtual void finalize(MemoryManager&, processor::PartitionerSharedState&) { KU_UNREACHABLE; }
    virtual void shrink(transaction::Transaction*, common::offset_t) { KU_UNREACHABLE; }
    virtual void shrinkToThreshold(transaction::Transaction*, common::offset_t, common::length_t) {
        KU_UNREACHABLE;
    }

protected:
    std::atomic<common::offset_t> entryPoint;
    common::offset_t numNodes;
    EmbeddingColumn* embeddings;
    DistFuncType distFunc;
};

// TODO: There should be a derived SparseInMemHNSWGraph, which is optimized for the upper graph of
// the index.
// Note: This is a special directed graph structure optimized for HNSW index. It is in-memory and
// keeps only forward directed relationships.
class InMemHNSWGraph final : public HNSWGraph {
public:
    InMemHNSWGraph(MemoryManager* mm, const HNSWGraphInfo& info, common::length_t maxDegree,
        common::length_t maxConnectivity, double alpha)
        : HNSWGraph{info}, maxDegree{maxDegree}, maxConnectivity{maxConnectivity}, alpha{alpha} {
        csrLengthBuffer = mm->allocateBuffer(true, numNodes * sizeof(std::atomic<uint16_t>));
        csrLengths = reinterpret_cast<std::atomic<uint16_t>*>(csrLengthBuffer->getData());
        dstNodesBuffer =
            mm->allocateBuffer(false, numNodes * maxDegree * sizeof(std::atomic<common::offset_t>));
        dstNodes = reinterpret_cast<std::atomic<common::offset_t>*>(dstNodesBuffer->getData());
        resetCSRLengthAndDstNodes();
    }

    common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset) const override;
    common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset, common::length_t numNbrs) const override;

    common::length_t getMaxDegree() const { return maxDegree; }

    void createDirectedEdge(transaction::Transaction* transaction, common::offset_t srcNode,
        common::offset_t dstNode) override;
    void shrink(transaction::Transaction* transaction, common::offset_t nodeOffset) override;
    void shrinkToThreshold(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::length_t numNbrs) override;

    void finalize(MemoryManager& mm,
        processor::PartitionerSharedState& partitionerSharedState) override;

private:
    void resetCSRLengthAndDstNodes();

    void finalizeNodeGroup(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        common::table_id_t srcNodeTableID, common::table_id_t dstNodeTableID,
        common::table_id_t relTableID, InMemChunkedNodeGroupCollection& partition) const;

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setCSRLength(common::offset_t nodeOffset, uint16_t length) {
        csrLengths[nodeOffset].store(length, std::memory_order_relaxed);
    }

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    uint16_t incrementCSRLength(common::offset_t nodeOffset) {
        return csrLengths[nodeOffset].fetch_add(1, std::memory_order_relaxed);
    }
    uint16_t getCSRLength(common::offset_t nodeOffset) const {
        return csrLengths[nodeOffset].load(std::memory_order_relaxed);
    }

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setDstNode(common::offset_t csrOffset, common::offset_t dstNode) {
        dstNodes[csrOffset].store(dstNode, std::memory_order_relaxed);
    }
    common::offset_t getDstNode(common::offset_t csrOffset) const {
        return dstNodes[csrOffset].load(std::memory_order_relaxed);
    }

private:
    std::unique_ptr<MemoryBuffer> csrLengthBuffer;
    std::unique_ptr<MemoryBuffer> dstNodesBuffer;
    std::atomic<uint16_t>* csrLengths;
    std::atomic<common::offset_t>* dstNodes;
    // Max allowed degree of a node in the graph before shrinking.
    common::length_t maxDegree;
    // Max allowed connectivity of a node in the graph after shrinking.
    common::length_t maxConnectivity;
    double alpha;

    std::vector<common::length_t> numRelsPerNodeGroup;
};

struct RelTableScanState;
class OnDiskHNSWGraph final : public HNSWGraph {
public:
    OnDiskHNSWGraph(main::ClientContext* context, const HNSWGraphInfo& info, NodeTable& nodeTable,
        RelTable& relTable);

    common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset) const override;
    common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset, common::length_t numNbrs) const override;

private:
    NodeTable& nodeTable;
    RelTable& relTable;

    // TODO(Guodong): Should move these to local state.
    // Cached data to read from the table.
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<RelTableScanState> scanState;
};

} // namespace storage
} // namespace kuzu
