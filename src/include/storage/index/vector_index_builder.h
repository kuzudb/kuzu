#pragma once

#include <unistd.h>

#include <queue>

#include "common/vector_index/distance_computer.h"
#include "common/vector_index/helpers.h"
#include "processor/operator/partitioner.h"
#include "storage/index/vector_index_header.h"

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
        types.emplace_back(*LogicalType::INTERNAL_ID());
        types.emplace_back(*LogicalType::INTERNAL_ID());
        types.emplace_back(*LogicalType::INTERNAL_ID());
        return ChunkedNodeGroupCollection(types);
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
    int maxNbrs;

    std::mutex mtx;
    int workingPartitionIdx = 0;
};

// This class is used to store the temporary vectors in the memory. It is used during
// the index building process.
struct VectorTempStorage {
    explicit VectorTempStorage(uint64_t dim, uint64_t numVectors)
        : dim(dim), numVectors(numVectors) {
        allocAligned((void**)&vectors, numVectors * dim * sizeof(float), 64);
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

class IndexKNN {
public:
    explicit IndexKNN(DistanceComputer* dc, int dim, int numEntries)
        : dc(dc), dim(dim), numEntries(numEntries){};

    void search(int k, const float* queries, double* distances, vector_id_t* resultIds);

private:
    DistanceComputer* dc;
    int dim;
    int numEntries;
};

// This graph is used to build the index and store the results in the VectorIndexGraph.
// Additionally, it is thread safe. So, `batchInsert` can be called from multiple threads.
// Currently, it implements the HNSW algorithm.
class VectorIndexBuilder {
public:
    explicit VectorIndexBuilder(VectorIndexHeader* header, VectorIndexGraph* graphStorage,
        VectorTempStorage* vectorTempStorage);

    // This function is used to insert batch of vectors into the index.
    void batchInsert(const float* vectors, const vector_id_t* vectorIds, uint64_t numVectors,
        VisitedTable* visited, DistanceComputer* dc);

    void search(const float* query, int k, int efSearch, VisitedTable* visited,
        std::priority_queue<NodeDistCloser>& results, DistanceComputer* dc);

private:
    void searchANN(DistanceComputer* dc, std::priority_queue<NodeDistCloser>& results,
        vector_id_t entrypoint, double entrypointDist, uint16_t efSearch, VisitedTable* visited,
        const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
        const std::function<vector_id_t(vector_id_t)>& getActualId);

    void searchNNOnUpperLevel(DistanceComputer* dc, vector_id_t& nearest, double& nearestDist);

    void shrinkNeighbors(DistanceComputer* dc, std::priority_queue<NodeDistCloser>& results,
        int maxSize, const std::function<vector_id_t(vector_id_t)>& getActualId);

    void makeConnection(DistanceComputer* dc, vector_id_t src, vector_id_t dest, double distSrcDest,
        int maxNbrs,
        const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
        const std::function<vector_id_t(vector_id_t)>& getActualId);

    void insertNode(DistanceComputer* dc, vector_id_t id, vector_id_t entrypoint,
        double entrypointDist, int maxNbrs, int efConstruction, VisitedTable* visited,
        std::vector<NodeDistCloser>& backNbrs,
        const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
        const std::function<vector_id_t(vector_id_t)>& getActualId);

    void insertNodeInUpperLayer(vector_id_t id, VisitedTable* visited, DistanceComputer* dc);

    void insertNodeInLowerLayer(vector_id_t id, VisitedTable* visited, DistanceComputer* dc);

    void findEntrypointUsingUpperLayer(DistanceComputer* dc, vector_id_t& entrypoint,
        double* entrypointDist);

private:
    VectorIndexHeader* header;
    VectorIndexGraph* graphStorage;
    VectorTempStorage* vectorTempStorage;
    std::vector<std::mutex> locks;
};

} // namespace storage
} // namespace kuzu
