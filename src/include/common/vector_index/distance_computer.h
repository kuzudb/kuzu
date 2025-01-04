#pragma once

#include <memory>

#include <common/types/types.h>
#include <simsimd/simsimd.h>
#include "main/client_context.h"
#include "storage/storage_manager.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"

using namespace kuzu::storage;

namespace kuzu {
namespace common {

struct DistanceComputer {
    virtual void computeDistance(vector_id_t id, double* result) = 0;

    virtual void computeDistance(vector_id_t src, vector_id_t dest, double* result) = 0;

    virtual void computeDistance(const float* dest, double* result) = 0;

    virtual void computeDistance(const float* src, const float* dest, double* result) = 0;

    virtual void batchComputeDistances(const float* dest, int size, double* results) = 0;

    virtual void batchComputeDistances(vector_id_t* ids, double* results, int size) = 0;

    virtual void setQuery(const float* query) = 0;

    virtual ~DistanceComputer() = default;
};

struct L2DistanceComputer : public DistanceComputer {
    explicit L2DistanceComputer(const float* data, int dim, int n)
        : data(data), dim(dim), n(n), query(nullptr) {}

    inline void computeDistance(vector_id_t id, double* result) override {
        KU_ASSERT_MSG(id < n, stringFormat("Index out of bounds {} < {}", id, n).data());
        const float* y = data + (id * dim);
        simsimd_l2sq_f32(query, y, dim, result);
    }

    inline void computeDistance(vector_id_t src, vector_id_t dest, double* result) override {
        KU_ASSERT_MSG(src < n && dest < n, "Index out of bounds");
        const float* x = data + (src * dim);
        const float* y = data + (dest * dim);
        simsimd_l2sq_f32(x, y, dim, result);
    }

    inline void computeDistance(const float* dest, double* result) override {
        simsimd_l2sq_f32(query, dest, dim, result);
    }

    inline void computeDistance(const float* src, const float* dest, double* result) override {
        simsimd_l2sq_f32(src, dest, dim, result);
    }

    inline void batchComputeDistances(const float* dest, int size, double* results) override {
        for (int i = 0; i < size; i++) {
            computeDistance(dest + (i * dim), &results[i]);
        }
    }

    inline void batchComputeDistances(vector_id_t* ids, double* results, int size) override {
        for (int i = 0; i < size; i++) {
            computeDistance(ids[i], &results[i]);
        }
    }

    inline void setQuery(const float* q) override { this->query = q; }

    ~L2DistanceComputer() override = default;

private:
    const float* data;
    int dim;
    int n;

    const float* query;
};

struct CosineDistanceComputer : public DistanceComputer {
    explicit CosineDistanceComputer(const float* data, int dim, int n)
            : data(data), dim(dim), n(n), query(nullptr) {}

    inline void computeDistance(vector_id_t id, double* result) override {
        KU_ASSERT_MSG(id < n, stringFormat("Index out of bounds {} < {}", id, n).data());
        const float* y = data + (id * dim);
        simsimd_cos_f32(query, y, dim, result);
    }

    inline void computeDistance(vector_id_t src, vector_id_t dest, double* result) override {
        KU_ASSERT_MSG(src < n && dest < n, "Index out of bounds");
        const float* x = data + (src * dim);
        const float* y = data + (dest * dim);
        simsimd_cos_f32(x, y, dim, result);
    }

    inline void computeDistance(const float* dest, double* result) override {
        simsimd_cos_f32(query, dest, dim, result);
    }

    inline void computeDistance(const float* src, const float* dest, double* result) override {
        simsimd_cos_f32(src, dest, dim, result);
    }

    inline void batchComputeDistances(const float* dest, int size, double* results) override {
        for (int i = 0; i < size; i++) {
            computeDistance(dest + (i * dim), &results[i]);
        }
    }

    inline void batchComputeDistances(vector_id_t* ids, double* results, int size) override {
        for (int i = 0; i < size; i++) {
            computeDistance(ids[i], &results[i]);
        }
    }

    inline void setQuery(const float* q) override { this->query = q; }

    ~CosineDistanceComputer() override = default;

private:
    const float* data;
    int dim;
    int n;

    const float* query;
};

struct NodeTableDistanceComputer: public DistanceComputer {
    explicit NodeTableDistanceComputer(main::ClientContext *context, table_id_t nodeTableId,
                                       property_id_t embeddingPropertyId, offset_t startOffset,
                                       std::unique_ptr<DistanceComputer> delegate)
                                       : context(context), delegate(std::move(delegate)), startOffset(startOffset) {
        auto nodeTable = ku_dynamic_cast<Table *, NodeTable *>(
                context->getStorageManager()->getTable(nodeTableId));
        embeddingColumn = nodeTable->getColumn(embeddingPropertyId);
        embeddingVector1 = std::make_unique<ValueVector>(embeddingColumn->getDataType().copy(),
                                                        context->getMemoryManager());
        embeddingVector1->state = DataChunkState::getSingleValueDataChunkState();
        embeddingVector2 = std::make_unique<ValueVector>(embeddingColumn->getDataType().copy(),
                                                        context->getMemoryManager());
        embeddingVector2->state = DataChunkState::getSingleValueDataChunkState();
        inputNodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID().copy(),
                                                          context->getMemoryManager());
        inputNodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
        auto totalNodeGroups = nodeTable->getNumCommittedNodeGroups();
        for (size_t i = 0; i < totalNodeGroups; i++) {
            readStates.push_back(std::make_unique<Column::ChunkState>());
        }
        // initialize only once
        for (size_t nodeGroup = 0; nodeGroup < totalNodeGroups; nodeGroup++) {
            auto readState = readStates[nodeGroup].get();
            embeddingColumn->initChunkState(context->getTx(), nodeGroup, *readState);
        }
    }

    inline void computeDistance(vector_id_t id, double *dist) override {
//        auto embedding = getEmbedding(id + startOffset, embeddingVector1.get());
//        delegate->computeDistance(embedding, dist);
        computeZeroCopyDistance(id + startOffset, delegate.get(), dist);
    }

    inline void computeDistance(vector_id_t src, vector_id_t dest, double *dist) override {
//        auto srcEmbedding = getEmbedding(src + startOffset, embeddingVector1.get());
//        auto destEmbedding = getEmbedding(dest + startOffset, embeddingVector2.get());
//        delegate->computeDistance(srcEmbedding, destEmbedding, dist);
        computeZeroCopyDistance(src + startOffset, dest + startOffset, delegate.get(), dist);
    }

    inline void computeDistance(const float* dest, double* result) override {
        delegate->computeDistance(dest, result);
    }

    inline void computeDistance(const float* src, const float* dest, double* result) override {
        delegate->computeDistance(src, dest, result);
    }

    inline void batchComputeDistances(const float* dest, int size, double* results) override {
        delegate->batchComputeDistances(dest, size, results);
    }

    inline void batchComputeDistances(vector_id_t* ids, double* results, int size) override {
        for (int i = 0; i < size; i++) {
            computeDistance(ids[i], &results[i]);
        }
    }

    inline void setQuery(const float *query) override {
        delegate->setQuery(query);
    }

    ~NodeTableDistanceComputer() override = default;

private:
    inline void computeZeroCopyDistance(vector_id_t id, DistanceComputer* dc, double *dist) {
        auto start = std::chrono::high_resolution_clock::now();
        auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

        // Initialize the read state
        auto readState = readStates[nodeGroupIdx].get();
        embeddingColumn->initChunkState(context->getTx(), nodeGroupIdx, *readState);
        // Fast compute on embedding
        // TODO: Add support for batch computation using io uring
        embeddingColumn->fastLookup(TransactionType::READ_ONLY, {readState}, {id},
                                    [dc, dist](std::vector<SeqFrames> &frames) {
                                        auto embedding = reinterpret_cast<const float *>(frames[0].frame);
                                        dc->computeDistance(embedding + frames[0].posInFrame, dist);
                                    });
    }

    inline void computeZeroCopyDistance(vector_id_t src, vector_id_t dest, DistanceComputer* dc, double *dist) {
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<Column::ChunkState*> states;
        std::vector<vector_id_t> ids;
        for (vector_id_t id : {src, dest}) {
            auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

            // Initialize the read state
            auto readState = readStates[nodeGroupIdx].get();
            states.emplace_back(readState);
            ids.emplace_back(id);
        }
        // Fast compute on embedding
        // TODO: Add support for batch computation using io uring
        embeddingColumn->fastLookup(TransactionType::READ_ONLY, states, ids,
                                    [dc, dist](std::vector<SeqFrames> &frames) {
                                        auto srcEmbedding =
                                                reinterpret_cast<const float *>(frames[0].frame) + frames[0].posInFrame;
                                        auto destEmbedding =
                                                reinterpret_cast<const float *>(frames[1].frame) + frames[1].posInFrame;
                                        dc->computeDistance(srcEmbedding, destEmbedding, dist);
                                    });
    }

    inline const float *getEmbedding(vector_id_t id, ValueVector* resultVector) {
        auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

        // Initialize the read state
        auto readState = readStates[nodeGroupIdx].get();
        // Read the embedding
        auto nodeIdVector = inputNodeIdVector.get();
        nodeIdVector->setValue(0, id);
        embeddingColumn->lookup(context->getTx(), *readState, nodeIdVector,
                                resultVector);
        return reinterpret_cast<float *>(ListVector::getDataVector(resultVector)->getData());
    }

private:
    main::ClientContext *context;
    std::unique_ptr<DistanceComputer> delegate;
    offset_t startOffset;

    Column *embeddingColumn;
    std::vector<std::unique_ptr<Column::ChunkState>> readStates;
    std::unique_ptr<ValueVector> inputNodeIdVector;
    std::unique_ptr<ValueVector> embeddingVector1;
    std::unique_ptr<ValueVector> embeddingVector2;
};

} // namespace common
} // namespace kuzu
