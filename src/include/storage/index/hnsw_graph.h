#pragma once

#include "common/uniq_lock.h"
#include "processor/operator/partitioner.h"
#include "storage/index/hnsw_config.h"
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

    virtual void initialize(main::ClientContext*, NodeTable&, common::column_id_t) {
        // DO NOTHING.
    }

protected:
    EmbeddingTypeInfo typeInfo;
};

class InMemEmbeddings final : public EmbeddingColumn {
public:
    InMemEmbeddings(MemoryManager& mm, EmbeddingTypeInfo typeInfo, common::offset_t numNodes,
        const common::LogicalType& columnType);

    float* getEmbedding(common::offset_t offset) const;
    ColumnChunkData& getData() const { return *data; }

    void initialize(main::ClientContext* context, NodeTable& table,
        common::column_id_t columnID) override;

private:
    std::unique_ptr<ColumnChunkData> data;
};

struct NodeTableScanState;
struct OnDiskEmbeddingScanState {
    common::DataChunk propertyVectors;
    std::unique_ptr<common::ValueVector> nodeIDVector;
    std::unique_ptr<NodeTableScanState> scanState;

    OnDiskEmbeddingScanState(MemoryManager* mm, NodeTable& nodeTable, common::column_id_t columnID);
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

    InMemHNSWGraph(common::offset_t numNodes, common::length_t maxDegree)
        : numNodes{numNodes}, maxDegree{maxDegree} {}
    virtual ~InMemHNSWGraph() = default;

    virtual common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset) = 0;
    virtual common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset, common::length_t numNbrs) = 0;

    common::length_t getMaxDegree() const { return maxDegree; }

    virtual uint16_t getNumRels(common::offset_t nodeOffset) = 0;
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    virtual void setNumRels(common::offset_t nodeOffset, uint16_t length) = 0;
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    virtual uint16_t incrementNumRels(common::offset_t nodeOffset) = 0;
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    virtual void setDstNode(common::offset_t nodeOffset, common::offset_t offsetInCSRList,
        common::offset_t dstNode) = 0;

    void finalize(MemoryManager& mm,
        const processor::PartitionerSharedState& partitionerSharedState);

private:
    void finalizeNodeGroup(MemoryManager& mm, common::node_group_idx_t nodeGroupIdx,
        common::table_id_t srcNodeTableID, common::table_id_t dstNodeTableID,
        common::table_id_t relTableID, InMemChunkedNodeGroupCollection& partition);

    virtual common::offset_t getDstNode(common::offset_t nodeOffset,
        common::offset_t offsetInCSRList) = 0;

protected:
    common::offset_t numNodes;
    // Max allowed degree of a node in the graph before shrinking.
    common::length_t maxDegree;

    std::vector<common::length_t> numRelsPerNodeGroup;
};

class SparseInMemHNSWGraph final : public InMemHNSWGraph {
public:
    SparseInMemHNSWGraph(common::offset_t numNodes, common::length_t maxDegree);

    common::offset_vec_t getNeighbors(transaction::Transaction*,
        common::offset_t nodeOffset) override;
    common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset, common::length_t numNbrs) override;

    uint16_t getNumRels(common::offset_t nodeOffset) override {
        auto& partition = getPartition(nodeOffset);
        auto lock = partition.lock();
        return partition.getNumRels(lock, nodeOffset);
    }
    void setNumRels(common::offset_t nodeOffset, uint16_t length) override {
        auto& partition = getPartition(nodeOffset);
        auto lock = partition.lock();
        partition.setNumRels(lock, nodeOffset, length);
    }
    uint16_t incrementNumRels(common::offset_t nodeOffset) override {
        auto& partition = getPartition(nodeOffset);
        auto lock = partition.lock();
        return partition.incrementNumRels(lock, nodeOffset);
    }
    void setDstNode(common::offset_t nodeOffset, common::offset_t offsetInCSRList,
        common::offset_t dstNode) override;
    common::offset_t getDstNode(common::offset_t nodeOffset,
        common::offset_t offsetInCSRList) override {
        auto& partition = getPartition(nodeOffset);
        auto lock = partition.lock();
        return partition.getDstNode(lock, nodeOffset, offsetInCSRList);
    }

private:
    static constexpr uint64_t PARTITION_SIZE = common::StorageConfig::NODE_GROUP_SIZE;
    struct Partition {
        common::UniqLock lock() const { return common::UniqLock{mtx}; }
        common::offset_vec_t getNeighbors([[maybe_unused]] const common::UniqLock& lock,
            common::offset_t nodeOffset) const {
            KU_ASSERT(lock.isLocked());
            return neighbors.contains(nodeOffset) ? neighbors.at(nodeOffset) :
                                                    common::offset_vec_t{};
        }
        common::offset_vec_t& getNeighbors([[maybe_unused]] const common::UniqLock& lock,
            common::offset_t nodeOffset) {
            KU_ASSERT(lock.isLocked());
            return neighbors.contains(nodeOffset) ? neighbors.at(nodeOffset) :
                                                    neighbors[nodeOffset];
        }
        common::offset_t getDstNode([[maybe_unused]] const common::UniqLock& lock,
            common::offset_t nodeOffset, common::offset_t offsetInCSRList) const {
            KU_ASSERT(lock.isLocked());
            return neighbors.contains(nodeOffset) &&
                           neighbors.at(nodeOffset).size() > offsetInCSRList ?
                       neighbors.at(nodeOffset)[offsetInCSRList] :
                       common::INVALID_OFFSET;
        }
        uint16_t getNumRels([[maybe_unused]] const common::UniqLock& lock,
            common::offset_t nodeOffset) const {
            KU_ASSERT(lock.isLocked());
            return neighbors.contains(nodeOffset) ? neighbors.at(nodeOffset).size() : 0;
        }
        void setNumRels([[maybe_unused]] const common::UniqLock& lock, common::offset_t nodeOffset,
            uint16_t length) {
            KU_ASSERT(lock.isLocked());
            if (neighbors.contains(nodeOffset) && neighbors.at(nodeOffset).size() > length) {
                neighbors.at(nodeOffset).resize(length);
            }
        }
        uint16_t incrementNumRels([[maybe_unused]] const common::UniqLock& lock,
            common::offset_t nodeOffset);

    private:
        mutable std::mutex mtx;
        std::unordered_map<common::offset_t, common::offset_vec_t> neighbors;
    };

    Partition& getPartition(common::offset_t nodeOffset);
    const Partition& getPartition(common::offset_t nodeOffset) const;

private:
    std::vector<std::unique_ptr<Partition>> partitions;
};

class DenseInMemHNSWGraph final : public InMemHNSWGraph {
public:
    DenseInMemHNSWGraph(MemoryManager* mm, common::offset_t numNodes, common::length_t maxDegree)
        : InMemHNSWGraph{numNodes, maxDegree} {
        csrLengthBuffer = mm->allocateBuffer(true, numNodes * sizeof(std::atomic<uint16_t>));
        csrLengths = reinterpret_cast<std::atomic<uint16_t>*>(csrLengthBuffer->getData());
        dstNodesBuffer =
            mm->allocateBuffer(false, numNodes * maxDegree * sizeof(std::atomic<common::offset_t>));
        dstNodes = reinterpret_cast<std::atomic<common::offset_t>*>(dstNodesBuffer->getData());
        resetCSRLengthAndDstNodes();
    }

    common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset) override;
    common::offset_vec_t getNeighbors(transaction::Transaction* transaction,
        common::offset_t nodeOffset, common::length_t numNbrs) override;

    uint16_t getNumRels(common::offset_t nodeOffset) override {
        return csrLengths[nodeOffset].load(std::memory_order_relaxed);
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setNumRels(common::offset_t nodeOffset, uint16_t length) override {
        csrLengths[nodeOffset].store(length, std::memory_order_relaxed);
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    uint16_t incrementNumRels(common::offset_t nodeOffset) override {
        return csrLengths[nodeOffset].fetch_add(1, std::memory_order_relaxed);
    }
    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const function.
    void setDstNode(common::offset_t nodeOffset, common::offset_t offsetInCSRList,
        common::offset_t dstNode) override {
        auto csrOffset = nodeOffset * maxDegree + offsetInCSRList;
        dstNodes[csrOffset].store(dstNode, std::memory_order_relaxed);
    }

private:
    void resetCSRLengthAndDstNodes();

    common::offset_t getDstNode(common::offset_t nodeOffset,
        common::offset_t offsetInCSRList) override {
        auto csrOffset = nodeOffset * maxDegree + offsetInCSRList;
        return dstNodes[csrOffset].load(std::memory_order_relaxed);
    }

private:
    std::unique_ptr<MemoryBuffer> csrLengthBuffer;
    std::unique_ptr<MemoryBuffer> dstNodesBuffer;
    std::atomic<uint16_t>* csrLengths;
    std::atomic<common::offset_t>* dstNodes;
};

} // namespace storage
} // namespace kuzu
