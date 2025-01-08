#pragma once

#include <unistd.h>

#include <queue>

#include "common/vector_index/distance_computer.h"
#include "common/vector_index/helpers.h"
#include "common/vector_index/vector_index_config.h"
#include "processor/operator/partitioner.h"
#include "storage/index/vector_index_header.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/store/list_chunk_data.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// TODO(gaurav): Potentially try to use use ColumnChunk here with node group such that
//  we can directly use chunk state.
class VectorIndexGraph {
public:
    explicit VectorIndexGraph(uint64_t numVectors, int maxNbrs, uint64_t partitionSize)
        : partitionSize(partitionSize), maxNbrs(maxNbrs) {
        uint64_t current = 0;
        while (current < numVectors) {
            auto partSize = std::min(partitionSize, numVectors - current);
            vector_id_t* nbrs;
            allocAligned((void**)&nbrs, partSize * maxNbrs * sizeof(vector_id_t), 64);
            // Initialize the neighbors to invalid vector id.
            for (size_t i = 0; i < partSize * maxNbrs; i++) {
                nbrs[i] = INVALID_VECTOR_ID;
            }
            current += partSize;
            partitionedNbrs.push_back(nbrs);
            partitionSizes.push_back(partSize);
        }
    }

    inline vector_id_t* getNeighbors(vector_id_t id, size_t& begin, size_t& end) {
        auto partition = getPartition(id);
        begin = (id - (partition * partitionSize)) * maxNbrs;
        end = begin + maxNbrs;
        return partitionedNbrs[partition];
    }

    inline int getPartition(vector_id_t id) const { return id / partitionSize; }

    void populatePartitionBuffer(processor::PartitioningBuffer& partitioningBuffer);

    inline ChunkedNodeGroupCollection createNodeGroupCollection() {
        std::vector<common::LogicalType> types;
        types.emplace_back(LogicalType::INTERNAL_ID());
        types.emplace_back(LogicalType::INTERNAL_ID());
        types.emplace_back(LogicalType::INTERNAL_ID());
        return ChunkedNodeGroupCollection(LogicalType::copy(types));
    }

    ~VectorIndexGraph() {
        printf("Deleting VectorIndexGraph\n");
        for (auto nbrs : partitionedNbrs) {
            free(nbrs);
        }
    }

private:
    std::vector<vector_id_t*> partitionedNbrs;
    std::vector<size_t> partitionSizes;
    uint64_t partitionSize;
    size_t maxNbrs;

    std::mutex mtx;
    int workingPartitionIdx = 0;
};

// This class is used to store the temporary vectors in the memory. It is used during
// the index building process.
struct VectorTempStorage {
    explicit VectorTempStorage(uint64_t dim, uint64_t numVectors)
        : dim(dim), numVectors(numVectors) {
        vectors = static_cast<float *>(malloc(numVectors * dim * sizeof(float)));
//        allocAligned((void**)&vectors, numVectors * dim * sizeof(float), 64);
    }

    ~VectorTempStorage() {
        printf("Deleting VectorTempStorage\n");
        free(vectors);
    }

    void copyVectors(const float* src, const vector_id_t* vectorIds, uint64_t numVectors) {
        // Try to copy bigger chunk if the vectorIds are contiguous
        std::vector<std::pair<vector_id_t, vector_id_t>> copyPair;
        vector_id_t start = vectorIds[0];
        vector_id_t end = start;
        for (size_t i = 1; i < numVectors; i++) {
            if (vectorIds[i] == end + 1) {
                end = vectorIds[i];
            } else {
                copyPair.emplace_back(start, end);
                start = vectorIds[i];
                end = start;
            }
        }
        copyPair.emplace_back(start, end);

        // TODO: Try using memcpy vectorized version
        auto prevNumVectors = 0;
        for (const auto& pair : copyPair) {
            auto numVecs = pair.second - pair.first + 1;
            memcpy(vectors + (pair.first * dim), src + (prevNumVectors * dim),
                numVecs * dim * sizeof(float));
            prevNumVectors += numVecs;
        }
    }

    const float* getVector(vector_id_t id) const { return vectors + (id * dim); }

    float* vectors;
    uint64_t dim;
    uint64_t numVectors;
};

// Struct to store compressed vectors with ColumnChunkData
struct CompressedVectorStorage {
    explicit CompressedVectorStorage(Transaction *transaction, Column *column,
                                     DiskArray<ColumnChunkMetadata> *metadataDA, size_t codeSize,
                                     node_group_idx_t startNodeGroupIdx, node_group_idx_t endNodeGroupIdx)
                                     : codeSize(codeSize), column(column), flushed(false) {
        KU_ASSERT(metadataDA != nullptr);
        // TODO: Make it generic, currently only supports scalar quantization
        auto dataType = LogicalType::ARRAY(LogicalType::INT8(), codeSize);
        for (auto nodeGroupIdx = startNodeGroupIdx; nodeGroupIdx <= endNodeGroupIdx; nodeGroupIdx++) {
            auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
            auto capacity = StorageConstants::NODE_GROUP_SIZE;
            auto numValues = chunkMeta.numValues;
            auto columnChunk = ColumnChunkFactory::createColumnChunkData(dataType.copy(), true, capacity);
            auto &listChunk = columnChunk->cast<ListChunkData>();
            listChunk.getDataColumnChunk()->resize(numValues * (codeSize));
            // Set offset and size column
            auto offsetColumn = listChunk.getOffsetColumnChunk();
            auto sizeColumn = listChunk.getSizeColumnChunk();

            // Initialize offset and size column
            uint64_t offset = 0;
            for (auto i = 0u; i < numValues; i++) {
                offset += (codeSize);
                offsetColumn->setValue(offset, i);
                sizeColumn->setValue((uint32_t) (codeSize), i);
            }
            listChunk.setNumValues(numValues);
            listChunk.getDataColumnChunk()->setNumValues(numValues * (codeSize));
            columnChunks.insert({nodeGroupIdx, std::move(columnChunk)});
        }
    }

    inline uint8_t* getData(node_group_idx_t nodeGroupIdx, offset_t offsetInNodeGroup) {
        KU_ASSERT(columnChunks.contains(nodeGroupIdx));
        return reinterpret_cast<ListChunkData *>(columnChunks[nodeGroupIdx].get())->getDataColumnChunk()->getData() +
               (codeSize * offsetInNodeGroup);
    }

    inline void flush(Transaction* trnx) {
        std::lock_guard<std::mutex> lock(mtx);
        if (flushed) {
            return;
        }
        printf("Flushing compressed data\n");
        Column::ChunkState state;
        for (auto& [nodeGroupIdx, columnChunk] : columnChunks) {
            printf("NodeGroupIdx: %d\n", nodeGroupIdx);
            auto data = reinterpret_cast<ListChunkData *>(columnChunks[nodeGroupIdx].get())->getDataColumnChunk()->getData();
            for (auto i = 0u; i < 50; i++) {
                printf("[%d] ", data[i]);
            }
            printf("\n");
            column->initChunkState(trnx, nodeGroupIdx, state);
            printf("Appending to column\n");
            printf("NumValues: %lu\n", state.metadata.numValues);
            column->append(columnChunk.get(), state);
        }
        flushed = true;
    }

private:
    std::unordered_map<node_group_idx_t, std::unique_ptr<ColumnChunkData>> columnChunks;
    size_t codeSize;

    std::mutex mtx;
    bool flushed = false;
    Column* column;
};


class Quantizer {
public:
    explicit Quantizer(SQ8Bit *sq, DistanceFunc distFunc, CompressedVectorStorage *compressedStorage, int batchSize,
                       int dim) : sq(sq), distFunc(distFunc), compressedStorage(compressedStorage), dim(dim),
                       batchSize(batchSize) {
        if (distFunc == DistanceFunc::COSINE) {
            normVectorsCache = static_cast<float *>(malloc(batchSize * dim * sizeof(float)));
        }
    }

    inline void batchTrain(const float *data, size_t n) {
        if (distFunc == DistanceFunc::COSINE) {
            normalizeVectors(data, n);
            sq->batchTrain(normVectorsCache, n);
        } else {
            sq->batchTrain(data, n);
        }
    }

    inline void finalizeTrain() { sq->finalizeTrain(); }

    inline void encode(const float *data, vector_id_t startVectorId, size_t n) {
        auto [nodeGroup, offsetInGroup] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(startVectorId);
        auto compressedData = compressedStorage->getData(nodeGroup, offsetInGroup);
        encode(data, compressedData, n);
    }

    ~Quantizer() {
        if (distFunc == DistanceFunc::COSINE) {
            free(normVectorsCache);
        }
    }

private:
    inline void normalizeVectors(const float* data, size_t n) {
        for (size_t i = 0; i < n; i++) {
            normalize_vectors(data + i * dim, dim, normVectorsCache + i * dim);
        }
    }

    inline void encode(const float *data, uint8_t *codes, size_t n) {
        KU_ASSERT(n <= batchSize);
        if (distFunc == DistanceFunc::COSINE) {
            normalizeVectors(data, n);
            sq->encode(normVectorsCache, codes, n);
        } else {
            sq->encode(data, codes, n);
        }
    }

private:
    SQ8Bit* sq;
    DistanceFunc distFunc;
    CompressedVectorStorage* compressedStorage;

    // For normalized vectors
    float* normVectorsCache;
    int dim;
    int batchSize;
};

// This graph is used to build the index and store the results in the VectorIndexGraph.
// Additionally, it is thread safe. So, `batchInsert` can be called from multiple threads.
// Currently, it implements the HNSW algorithm.
class VectorIndexBuilder {
public:
    explicit VectorIndexBuilder(VectorIndexHeader *header, int partitionId, vector_id_t numVectors,
                                VectorIndexGraph *graphStorage);

    // This function is used to insert batch of vectors into the index.
    void batchInsert(const float* vectors, const vector_id_t* vectorIds, uint64_t numVectors,
        VisitedTable* visited, NodeTableDistanceComputer<float>* dc);

    void search(const float* query, int k, int efSearch, VisitedTable* visited,
        std::priority_queue<NodeDistCloser>& results, NodeTableDistanceComputer<float>* dc);

private:
    void searchANN(NodeTableDistanceComputer<float>* dc, std::priority_queue<NodeDistCloser>& results,
        vector_id_t entrypoint, double entrypointDist, uint16_t efSearch, VisitedTable* visited,
        const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
        const std::function<vector_id_t(vector_id_t)>& getActualId);

    void searchNNOnUpperLevel(NodeTableDistanceComputer<float>* dc, vector_id_t& nearest, double& nearestDist);

    void shrinkNeighbors(NodeTableDistanceComputer<float>* dc, std::priority_queue<NodeDistCloser>& results,
        int maxSize, const std::function<vector_id_t(vector_id_t)>& getActualId);

    void makeConnection(NodeTableDistanceComputer<float>* dc, vector_id_t src, vector_id_t dest, double distSrcDest,
        int maxNbrs,
        const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
        const std::function<vector_id_t(vector_id_t)>& getActualId);

    void insertNode(NodeTableDistanceComputer<float>* dc, vector_id_t id, vector_id_t entrypoint,
        double entrypointDist, int maxNbrs, int efConstruction, VisitedTable* visited,
        std::vector<NodeDistCloser>& backNbrs,
        const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
        const std::function<vector_id_t(vector_id_t)>& getActualId);

    void insertNodeInUpperLayer(const float* vector, vector_id_t id, VisitedTable* visited, NodeTableDistanceComputer<float>* dc);

    void insertNodeInLowerLayer(const float* vector, vector_id_t id, VisitedTable* visited, NodeTableDistanceComputer<float>* dc);

    void findEntrypointUsingUpperLayer(NodeTableDistanceComputer<float>* dc, vector_id_t& entrypoint,
        double* entrypointDist);

private:
    VectorIndexHeader* header;
    VectorIndexHeaderPerPartition* partitionHeader;
    VectorIndexGraph* graphStorage;
//    VectorTempStorage* vectorTempStorage;
    std::vector<std::mutex> locks;
    std::atomic<uint64_t> nodeCounter;
};

} // namespace storage
} // namespace kuzu
