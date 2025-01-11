#pragma once

#include <memory>

#include <common/types/types.h>
#include <simsimd/simsimd.h>
#include "main/client_context.h"
#include "common/vector_index/helpers.h"
#include "storage/storage_manager.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"
#include "storage/index/quantization.h"

using namespace kuzu::storage;

namespace kuzu {
namespace common {

template<typename T>
struct DistanceComputer {
    int dim;

    explicit DistanceComputer(int dim) : dim(dim) {}

    virtual void computeDistance(const T* dest, double* result) = 0;

    virtual void computeDistance(const T* src, const T* dest, double* result) = 0;

    virtual void setQuery(const float* query) = 0;

    virtual ~DistanceComputer() = default;
};

struct L2DistanceComputer : public DistanceComputer<float> {
    explicit L2DistanceComputer(int dim)
        : DistanceComputer<float>(dim) {}

    inline void computeDistance(const float* dest, double* result) override {
        simsimd_l2sq_f32(query, dest, dim, result);
    }

    inline void computeDistance(const float* src, const float* dest, double* result) override {
        simsimd_l2sq_f32(src, dest, dim, result);
    }

    inline void setQuery(const float* query) override {
        this->query = query;
    }

    ~L2DistanceComputer() override = default;

private:
    const float* query;
};

struct IPDistanceComputer : public DistanceComputer<float> {
    explicit IPDistanceComputer(int dim) : DistanceComputer<float>(dim) {}

    inline void computeDistance(const float* dest, double* result) override {
        simsimd_dot_f32(query, dest, dim, result);
    }

    inline void computeDistance(const float* src, const float* dest, double* result) override {
        simsimd_dot_f32(src, dest, dim, result);
    }

    inline void setQuery(const float* query) override {
        this->query = query;
    }

    ~IPDistanceComputer() override = default;

private:
    const float* query;
};

struct CosineDistanceComputer : public DistanceComputer<float> {
    explicit CosineDistanceComputer(int dim) : DistanceComputer<float>(dim) {}

    inline void computeDistance(const float* dest, double* result) override {
        simsimd_cos_f32(query, dest, dim, result);
    }

    inline void computeDistance(const float* src, const float* dest, double* result) override {
        simsimd_cos_f32(src, dest, dim, result);
    }

    inline void setQuery(const float* query) override {
        this->query = query;
    }

    ~CosineDistanceComputer() override = default;

private:
    const float* query;
};

class SQ8AsymL2Sq : public DistanceComputer<uint8_t> {
public:
    explicit SQ8AsymL2Sq(int dim, const float *alpha, const float *beta, const float *alphaSqr)
            : DistanceComputer(dim), alpha(alpha), beta(beta), alphaSqr(alphaSqr) {};

    ~SQ8AsymL2Sq() override = default;

    inline void computeDistance(const uint8_t* dest, double* result) override {
#if SIMSIMD_TARGET_SKYLAKE
        compute_asym_l2sq_skylake_8bit(query, dest, result, dim, alpha, beta);
#else
        compute_asym_l2sq_serial_8bit(query, dest, result, dim, alpha, beta);
#endif
    }

    inline void computeDistance(const uint8_t* src, const uint8_t* dest, double *result) override {
#if SIMSIMD_TARGET_SKYLAKE
        compute_sym_l2sq_skylake_8bit(src, dest, result, dim, alphaSqr);
#else
        compute_sym_l2sq_serial_8bit(src, dest, result, dim, alphaSqr);
#endif
    }

    inline void setQuery(const float* query) override {
        this->query = query;
    }

private:
    const float *alpha;
    const float *beta;
    const float *alphaSqr;

    const float* query;
};

class SQ8SymL2Sq : public DistanceComputer<uint8_t> {
public:
    explicit SQ8SymL2Sq(int dim, const SQ8Bit* quantizer)
            : DistanceComputer(dim), quantizer(quantizer) {
        quantizedQuery = new uint8_t[quantizer->codeSize];
    };

    ~SQ8SymL2Sq() override {
        delete[] quantizedQuery;
    }

    inline void computeDistance(const uint8_t* dest, double* result) override {
#if SIMSIMD_TARGET_SKYLAKE
        compute_sym_l2sq_skylake_8bit(quantizedQuery, dest, result, dim, quantizer->getAlphaSqr());
#else
        compute_sym_l2sq_serial_8bit(quantizedQuery, dest, result, dim, quantizer->getAlphaSqr());
#endif
    }

    inline void computeDistance(const uint8_t* src, const uint8_t* dest, double *result) override {
#if SIMSIMD_TARGET_SKYLAKE
        compute_sym_l2sq_skylake_8bit(src, dest, result, dim, quantizer->getAlphaSqr());
#else
        compute_sym_l2sq_serial_8bit(src, dest, result, dim, quantizer->getAlphaSqr());
#endif
    }

    inline void setQuery(const float * query) override {
        quantizer->encode(query, quantizedQuery, 1);
    }

private:
    const SQ8Bit* quantizer;
    uint8_t* quantizedQuery;
};

class SQ8AsymIP : public DistanceComputer<uint8_t> {
public:
    explicit SQ8AsymIP(int dim, const float *alpha, const float *beta, const float *alphaSqr)
            : DistanceComputer(dim), alpha(alpha), beta(beta), alphaSqr(alphaSqr) {};

    inline void computeDistance(const uint8_t* dest, double* result) override {
        compute_asym_ip_serial_8bit(query, dest, result, dim, alpha, beta);
    }

    inline void computeDistance(const uint8_t* src, const uint8_t* dest, double *result) override {
        compute_sym_ip_serial_8bit(src, dest, result, dim, alphaSqr);
    }

    inline void setQuery(const float* query) override {
        this->query = query;
    }

private:
    const float *alpha;
    const float *beta;
    const float *alphaSqr;

    const float* query;
};

class SQ8SymIP : public DistanceComputer<uint8_t> {
public:
    explicit SQ8SymIP(int dim, const SQ8Bit* quantizer)
            : DistanceComputer(dim), quantizer(quantizer) {
        quantizedQuery = new uint8_t[quantizer->codeSize];
    };

    ~SQ8SymIP() override {
        delete[] quantizedQuery;
    }

    inline void computeDistance(const uint8_t* dest, double* result) override {
        compute_sym_ip_serial_8bit(quantizedQuery, dest, result, dim, quantizer->getAlphaSqr());
    }

    inline void computeDistance(const uint8_t* src, const uint8_t* dest, double *result) override {
        compute_sym_ip_serial_8bit(src, dest, result, dim, quantizer->getAlphaSqr());
    }

    inline void setQuery(const float * query) override {
        quantizer->encode(query, quantizedQuery, 1);
    }

private:
    const SQ8Bit* quantizer;
    uint8_t* quantizedQuery;
};

class SQ8AsymCosine : public DistanceComputer<uint8_t> {
public:
    explicit SQ8AsymCosine(int dim, const float *alpha, const float *beta, const float *alphaSqr)
            : DistanceComputer(dim), alpha(alpha), beta(beta), alphaSqr(alphaSqr) {
        query = new float[dim];
    };

    ~SQ8AsymCosine() override {
        delete[] query;
    }

    inline void computeDistance(const uint8_t* dest, double* result) override {
#if SIMSIMD_TARGET_SKYLAKE
        compute_asym_ip_skylake_8bit(query, dest, result, dim, alpha, beta);
#else
        compute_asym_ip_serial_8bit(query, dest, result, dim, alpha, beta);
#endif
        *result = 1 - *result;
        *result = *result < 0 ? 0 : *result;
    }

    inline void computeDistance(const uint8_t* src, const uint8_t* dest, double *result) override {
        compute_sym_ip_serial_8bit(src, dest, result, dim, alphaSqr);
        *result = 1 - *result;
        *result = *result < 0 ? 0 : *result;
    }

    inline void setQuery(const float* query) override {
        normalize_vectors(query, dim, this->query);
    }

private:
    const float *alpha;
    const float *beta;
    const float *alphaSqr;
    float* query;
};

class SQ8SymCosine : public DistanceComputer<uint8_t> {
public:
    explicit SQ8SymCosine(int dim, const SQ8Bit* quantizer)
            : DistanceComputer(dim), quantizer(quantizer) {
        normalizedQuery = new float[dim];
        quantizedQuery = new uint8_t[quantizer->codeSize];
    };

    ~SQ8SymCosine() override {
        delete[] normalizedQuery;
        delete[] quantizedQuery;
    }

    inline void computeDistance(const uint8_t* dest, double* result) override {
#if SIMSIMD_TARGET_SKYLAKE
        compute_sym_ip_skylake_8bit(quantizedQuery, dest, result, dim, quantizer->getAlphaSqr());
#else
        compute_sym_ip_serial_8bit(quantizedQuery, dest, result, dim, quantizer->getAlphaSqr());
#endif
        *result = 1 - *result;
        *result = *result < 0 ? 0 : *result;
    }

    inline void computeDistance(const uint8_t* src, const uint8_t* dest, double *result) override {
        compute_sym_ip_serial_8bit(src, dest, result, dim, quantizer->getAlphaSqr());
        *result = 1 - *result;
        *result = *result < 0 ? 0 : *result;
    }

    inline void setQuery(const float * query) override {
        normalize_vectors(query, dim, normalizedQuery);
        quantizer->encode(normalizedQuery, quantizedQuery, 1);
    }

private:
    const SQ8Bit* quantizer;
    float* normalizedQuery;
    uint8_t* quantizedQuery;
};

template<typename T>
struct NodeTableDistanceComputer {
    explicit NodeTableDistanceComputer(main::ClientContext *context, table_id_t nodeTableId,
                                       property_id_t embeddingPropertyId, offset_t startOffset,
                                       std::unique_ptr<DistanceComputer<T>> delegate)
                                       : context(context), startOffset(startOffset), delegate(std::move(delegate)) {
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
        for (auto nodeGroup = 0; nodeGroup < totalNodeGroups; nodeGroup++) {
            auto readState = readStates[nodeGroup].get();
            embeddingColumn->initChunkState(context->getTx(), nodeGroup, *readState);
//            // print state.metadata.pageIdx
//            printf("state.metadata.pageIdx: %d\n", readState->metadata.pageIdx);
//            // print state.numValuesPerPage
//            printf("state.numValuesPerPage: %d\n", readState->numValuesPerPage);
        }
        readRequest = std::make_unique<Column::FastLookupRequest>();
    }

    inline void computeDistance(vector_id_t id, double* result) {
//        auto embedding = getEmbedding(id + startOffset, embeddingVector1.get());
//        delegate->computeDistance(embedding, dist);
        computeZeroCopyDistance(id + startOffset, delegate.get(), result);
    }

    inline void computeDistance(vector_id_t src, vector_id_t dest, double* result) {
//        auto srcEmbedding = getEmbedding(src + startOffset, embeddingVector1.get());
//        auto destEmbedding = getEmbedding(dest + startOffset, embeddingVector2.get());
//        delegate->computeDistance(srcEmbedding, destEmbedding, dist);
        computeZeroCopyDistance(src + startOffset, dest + startOffset, delegate.get(), result);
    }

    inline void batchComputeDistance(const vector_id_t* vecIds, const int numIds, double* results) {
        KU_ASSERT(numIds <= common::FAST_LOOKUP_MAX_BATCH_SIZE && numIds >= 4);
        batchComputeZeroCopyDistance(vecIds, numIds, delegate.get(), results);
    }

    inline void setQuery(const float* query) {
        delegate->setQuery(query);
    }

private:
    inline void computeZeroCopyDistance(vector_id_t id, DistanceComputer<T>* dc, double *dist) {
        auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);
//        printf("nodeGroupIdx: %d, offsetInChunk: %d\n", nodeGroupIdx, offsetInChunk);

        // Initialize the read state
        readRequest->states[0] = readStates[nodeGroupIdx].get();
        readRequest->nodeOffsets[0] = id;
        readRequest->size = 1;
        // Fast compute on embedding
        // TODO: Add support for batch computation using io uring
        embeddingColumn->fastLookup(TransactionType::READ_ONLY, readRequest.get(),
                                    [dc, dist](std::array<const uint8_t *, common::FAST_LOOKUP_MAX_BATCH_SIZE> frames,
                                               std::array<uint16_t, common::FAST_LOOKUP_MAX_BATCH_SIZE> positionInFrame,
                                               int size) {
                                        KU_ASSERT(size == 1);
                                        auto embedding = reinterpret_cast<const T *>(frames[0]) + positionInFrame[0];
                                        dc->computeDistance(embedding, dist);
                                    });
    }

    inline void computeZeroCopyDistance(vector_id_t src, vector_id_t dest, DistanceComputer<T>* dc, double *dist) {
        int idx = 0;
        for (vector_id_t id : {src, dest}) {
            auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);
            // Initialize the read state
            readRequest->states[idx] = readStates[nodeGroupIdx].get();
            readRequest->nodeOffsets[idx] = id;
            idx++;
        }
        readRequest->size = 2;
        // Fast compute on embedding
        // TODO: Add support for batch computation using io uring
        embeddingColumn->fastLookup(TransactionType::READ_ONLY, readRequest.get(),
                                    [dc, dist](std::array<const uint8_t *, common::FAST_LOOKUP_MAX_BATCH_SIZE> frames,
                                               std::array<uint16_t, common::FAST_LOOKUP_MAX_BATCH_SIZE> positionInFrame,
                                               int size) {
                                        KU_ASSERT(size == 2);
                                        auto srcEmbedding =
                                                reinterpret_cast<const T *>(frames[0]) + positionInFrame[0];
                                        auto destEmbedding =
                                                reinterpret_cast<const T *>(frames[1]) + positionInFrame[1];
                                        dc->computeDistance(srcEmbedding, destEmbedding, dist);
                                    });
    }

    inline void batchComputeZeroCopyDistance(const vector_id_t *vecIds, const int numIds, DistanceComputer<T> *dc,
                                             double *results) {
        // Initialize the read state
        for (int i = 0; i < numIds; i++) {
            auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(vecIds[i]);
            readRequest->states[i] = readStates[nodeGroupIdx].get();
            readRequest->nodeOffsets[i] = vecIds[i];
        }
        readRequest->size = numIds;
        // Fast compute on embedding
        embeddingColumn->fastLookup(TransactionType::READ_ONLY, readRequest.get(),
                                    [dc, results, numIds](std::array<const uint8_t *, common::FAST_LOOKUP_MAX_BATCH_SIZE> frames,
                                                   std::array<uint16_t, common::FAST_LOOKUP_MAX_BATCH_SIZE> positionInFrame,
                                                   int size) {
                                        KU_ASSERT(size == numIds);
                                        int i = 0;
                                        for (; i + 4 <= size; i += 4) {
                                            auto embedding0 = reinterpret_cast<const T *>(frames[i]) + positionInFrame[i];
                                            dc->computeDistance(embedding0, results + i);

                                            auto embedding1 = reinterpret_cast<const T *>(frames[i + 1]) + positionInFrame[i + 1];
                                            dc->computeDistance(embedding1, results + i + 1);

                                            auto embedding2 = reinterpret_cast<const T *>(frames[i + 2]) + positionInFrame[i + 2];
                                            dc->computeDistance(embedding2, results + i + 2);

                                            auto embedding3 = reinterpret_cast<const T *>(frames[i + 3]) + positionInFrame[i + 3];
                                            dc->computeDistance(embedding3, results + i + 3);
                                        }
                                    });
    }

    inline const T* getEmbedding(vector_id_t id, ValueVector* resultVector) {
        auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);
        // Initialize the read state
        auto readState = readStates[nodeGroupIdx].get();
        // Read the embedding
        auto nodeIdVector = inputNodeIdVector.get();
        nodeIdVector->setValue(0, id);
        embeddingColumn->lookup(context->getTx(), *readState, nodeIdVector,
                                resultVector);
        return reinterpret_cast<T*>(ListVector::getDataVector(resultVector)->getData());
    }

private:
    main::ClientContext *context;
    offset_t startOffset;
    std::unique_ptr<DistanceComputer<T>> delegate;

    Column *embeddingColumn;
    std::vector<std::unique_ptr<Column::ChunkState>> readStates;
    std::unique_ptr<ValueVector> inputNodeIdVector;
    std::unique_ptr<ValueVector> embeddingVector1;
    std::unique_ptr<ValueVector> embeddingVector2;
    std::unique_ptr<Column::FastLookupRequest> readRequest;
};

static inline std::unique_ptr<NodeTableDistanceComputer<float>> createDistanceComputer(
        main::ClientContext *context, table_id_t nodeTableId, property_id_t embeddingPropertyId, offset_t startOffset,
        int dim, DistanceFunc distanceType) {
    std::unique_ptr<DistanceComputer<float>> delegate;
    if (distanceType == DistanceFunc::L2) {
        delegate =  std::make_unique<L2DistanceComputer>(dim);
    } else if (distanceType == DistanceFunc::IP) {
        delegate = std::make_unique<IPDistanceComputer>(dim);
    } else if (distanceType == DistanceFunc::COSINE) {
        delegate =  std::make_unique<CosineDistanceComputer>(dim);
    }

    return std::make_unique<NodeTableDistanceComputer<float>>(context, nodeTableId, embeddingPropertyId, startOffset,
                                                              std::move(delegate));
}

static inline std::unique_ptr<NodeTableDistanceComputer<uint8_t>> createQuantizedDistanceComputer(
        main::ClientContext *context, table_id_t nodeTableId, property_id_t quantizedEmbeddingPropertyId,
        offset_t startOffset,
        int dim, DistanceFunc distanceType, const SQ8Bit *quantizer, bool symmetric = false) {
    std::unique_ptr<DistanceComputer<uint8_t>> delegate;
    if (distanceType == DistanceFunc::L2) {
        if (symmetric) {
            delegate = std::make_unique<SQ8SymL2Sq>(dim, quantizer);
        } else {
            delegate = std::make_unique<SQ8AsymL2Sq>(dim, quantizer->getAlpha(), quantizer->getBeta(),
                                                     quantizer->getAlphaSqr());
        }
    } else if (distanceType == DistanceFunc::IP) {
        if (symmetric) {
            delegate = std::make_unique<SQ8SymIP>(dim, quantizer);
        } else {
            delegate = std::make_unique<SQ8AsymIP>(dim, quantizer->getAlpha(), quantizer->getBeta(),
                                                   quantizer->getAlphaSqr());
        }
    } else if (distanceType == DistanceFunc::COSINE) {
        // Cosine distance is same as IP distance for normalized vectors
        if (symmetric) {
            delegate = std::make_unique<SQ8SymCosine>(dim, quantizer);
        } else {
            delegate = std::make_unique<SQ8AsymCosine>(dim, quantizer->getAlpha(), quantizer->getBeta(),
                                                   quantizer->getAlphaSqr());
        }
    }

    return std::make_unique<NodeTableDistanceComputer<uint8_t>>(context, nodeTableId, quantizedEmbeddingPropertyId,
                                                                startOffset,
                                                                std::move(delegate));
}

} // namespace common
} // namespace kuzu
