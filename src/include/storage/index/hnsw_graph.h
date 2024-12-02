#pragma once

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

    // Cacheed data structures used to read from table.
    common::DataChunk propertyVectors;
    std::unique_ptr<common::ValueVector> nodeIDVector;
    std::unique_ptr<NodeTableScanState> scanState;
};

struct NodeWithDistance {
    common::offset_t nodeOffset;
    double_t distance;
};

class HNSWGraph {
public:
    HNSWGraph(common::offset_t numNodes, common::length_t maxDegree, EmbeddingColumn* embeddings)
        : entryPoint{common::INVALID_OFFSET}, numNodes{numNodes}, maxDegree{maxDegree},
          embeddings{embeddings} {}
    virtual ~HNSWGraph() = default;

    virtual common::offset_vec_t getNeighbors(common::offset_t node,
        transaction::Transaction* transaction) const = 0;

    void setEntryPoint(common::offset_t entryPoint_) { this->entryPoint = entryPoint_; }

    common::offset_t getEntryPoint() const { return entryPoint; }
    common::offset_t getNumNodes() const { return numNodes; }
    common::length_t getMaxDegree() const { return maxDegree; }

    virtual void createDirectedEdge(common::offset_t, common::offset_t, transaction::Transaction*) {
        KU_UNREACHABLE;
    }
    virtual void finalize(MemoryManager&, processor::PartitionerSharedState&) { KU_UNREACHABLE; }
    virtual void shrinkToThreshold(common::offset_t, transaction::Transaction*) { KU_UNREACHABLE; }

protected:
    common::offset_t entryPoint;
    common::offset_t numNodes;
    common::length_t maxDegree;
    EmbeddingColumn* embeddings;
};

// TODO: There should be a derived SparseInMemHNSWGraph, which is optimized for the upper graph of
// the index.
// TODO: Not thread-safe for now.
// Note: This is a special directed graph structure optimized for HNSW index. It is in-memory and
// keeps only forward directed relationships.
class InMemHNSWGraph final : public HNSWGraph {
public:
    static constexpr double ALPHA = 1.10;

    InMemHNSWGraph(common::offset_t numNodes, common::length_t maxDegree,
        common::length_t shrinkThreshold, EmbeddingColumn* embeddings)
        : HNSWGraph{numNodes, maxDegree, embeddings}, shrinkThreshold{shrinkThreshold} {
        csrLengths = std::make_unique<uint16_t[]>(numNodes);
        dstNodes = std::make_unique<common::offset_t[]>(numNodes * maxDegree);
        resetCSRLengthAndDstNodes();
    }

    common::offset_vec_t getNeighbors(common::offset_t node,
        transaction::Transaction* transaction) const override;

    void createDirectedEdge(common::offset_t srcNode, common::offset_t dstNode,
        transaction::Transaction* transaction) override;
    void shrinkNbrs(common::offset_t offset, transaction::Transaction* transaction);
    void shrinkToThreshold(common::offset_t offset, transaction::Transaction* transaction) override;

    void finalize(MemoryManager& mm,
        processor::PartitionerSharedState& partitionerSharedState) override;

private:
    void resetCSRLengthAndDstNodes();

    void finalizeNodeGroup(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        common::table_id_t srcNodeTableID, common::table_id_t dstNodeTableID,
        common::table_id_t relTableID, InMemChunkedNodeGroupCollection& partition) const;

private:
    // TODO: We can relax uint16_t here to uint32_t if we allow maxDegree to be larger than 65535.
    // TODO: We can also dynamically change offset_t to smaller type if numNodes is smaller.
    // TODO: Should make use of MemoryBuffer to store these, so the used memory here is trackable in
    // the system.
    std::unique_ptr<uint16_t[]> csrLengths;
    std::unique_ptr<common::offset_t[]> dstNodes;
    common::length_t shrinkThreshold;

    std::vector<common::length_t> numRelsPerNodeGroup;
};

struct RelTableScanState;
class OnDiskHNSWGraph final : public HNSWGraph {
public:
    OnDiskHNSWGraph(main::ClientContext* context, common::length_t maxDegree, NodeTable& nodeTable,
        RelTable& relTable, EmbeddingColumn* embeddings);

    common::offset_vec_t getNeighbors(common::offset_t offset,
        transaction::Transaction* transaction) const override;

private:
    NodeTable& nodeTable;
    RelTable& relTable;

    // Cached data to read from the table.
    std::unique_ptr<common::ValueVector> srcNodeIDVector;
    std::unique_ptr<common::ValueVector> dstNodeIDVector;
    std::unique_ptr<RelTableScanState> scanState;
};

} // namespace storage
} // namespace kuzu
