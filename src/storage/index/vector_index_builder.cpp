#include "storage/index/vector_index_builder.h"

#include "common/prefetch.h"
#include "processor/operator/partitioner.h"

namespace kuzu {
namespace storage {

void VectorIndexGraph::populatePartitionBuffer(processor::PartitioningBuffer& partitioningBuffer) {
    while (true) {
        int workingPartition;
        {
            std::lock_guard<std::mutex> lock(mtx);
            if (workingPartitionIdx >= partitionedNbrs.size()) {
                return;
            }
            partitioningBuffer.partitions.emplace_back(createNodeGroupCollection());
            workingPartition = workingPartitionIdx++;
        }

        offset_t initialFromId = workingPartition * partitionSize;
        offset_t relIdx = initialFromId * maxNbrs;
        vector_id_t* neighbours = partitionedNbrs[workingPartition];
        auto neighbourSize = partitionSizes[workingPartition];
        // Calculate capacity of the partition
        auto capacity = 0;
        for (size_t i = 0; i < neighbourSize * maxNbrs; i++) {
            if (neighbours[i] != INVALID_VECTOR_ID) {
                capacity++;
            }
        }

        // Copy the neighbors to the partition buffer
        auto& collection = partitioningBuffer.partitions[workingPartition];
        auto chunkGroup =
            std::make_unique<ChunkedNodeGroup>(collection.getTypes(), false, capacity);
        auto& from = chunkGroup->getColumnChunkUnsafe(0);
        auto& to = chunkGroup->getColumnChunkUnsafe(1);
        auto& relIdxColumn = chunkGroup->getColumnChunkUnsafe(2);
        offset_t idx = 0;
        for (size_t i = 0; i < neighbourSize; i++) {
            for (size_t j = 0; j < maxNbrs; j++) {
                if (neighbours[i * maxNbrs + j] != INVALID_VECTOR_ID) {
                    from.setValue<offset_t>(initialFromId, idx);
                    to.setValue<offset_t>(neighbours[i * maxNbrs + j], idx);
                    relIdxColumn.setValue<offset_t>(relIdx, idx);
                    relIdx++;
                    idx++;
                }
            }
            initialFromId++;
        }
        chunkGroup->setNumRows(capacity);
        collection.merge(std::move(chunkGroup));
    }
}

VectorIndexBuilder::VectorIndexBuilder(VectorIndexHeader* header, int partitionId, vector_id_t numVectors,
                                       VectorIndexGraph* graphStorage)
    : header(header), graphStorage(graphStorage),
      locks(std::vector<std::mutex>(numVectors)), nodeCounter(0) {
    partitionHeader = header->getPartitionHeader(partitionId);
}

void VectorIndexBuilder::searchNNOnUpperLevel(NodeTableDistanceComputer<float>* dc, vector_id_t& nearest,
    double& nearestDist) {
    while (true) {
        vector_id_t prev_nearest = nearest;
        size_t begin, end;
        auto neighbors = partitionHeader->getNeighbors(nearest, begin, end);
        for (size_t i = begin; i < end; i++) {
            vector_id_t neighbor = neighbors[i];
            if (neighbor == INVALID_VECTOR_ID) {
                break;
            }
            double dist;
            dc->computeDistance(partitionHeader->getActualId(neighbor), &dist);
            if (dist < nearestDist) {
                nearest = neighbor;
                nearestDist = dist;
            }
        }
        if (prev_nearest == nearest) {
            break;
        }
    }
}

void VectorIndexBuilder::searchANN(NodeTableDistanceComputer<float>* dc,
    std::priority_queue<NodeDistCloser>& results, vector_id_t entrypoint, double entrypointDist,
    uint16_t efSearch, VisitedTable* visited,
    const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
    const std::function<vector_id_t(vector_id_t)>& getActualId) {
    std::priority_queue<NodeDistFarther> candidates;
    candidates.emplace(entrypoint, entrypointDist);
    results.emplace(entrypoint, entrypointDist);
    visited->set(entrypoint);
    while (!candidates.empty()) {
        auto candidate = candidates.top();
        if (candidate.dist > results.top().dist) {
            break;
        }
        candidates.pop();
        size_t begin, end;
        auto neighbors = getNeighbours(candidate.id, begin, end);

        size_t jmax = begin;
        for (size_t i = begin; i < end; i++) {
            vector_id_t neighbor = neighbors[i];
            if (neighbor == INVALID_VECTOR_ID) {
                break;
            }
            prefetch_L3(visited->data() + neighbor);
            jmax += 1;
        }

        for (size_t i = begin; i < jmax; i++) {
            vector_id_t neighbor = neighbors[i];
            if (neighbor == INVALID_VECTOR_ID) {
                break;
            }
            if (visited->get(neighbor)) {
                continue;
            }
            visited->set(neighbor);
            double dist;
            dc->computeDistance(getActualId(neighbor), &dist);
            if (results.size() < efSearch || dist < results.top().dist) {
                candidates.emplace(neighbor, dist);
                results.emplace(neighbor, dist);
                if (results.size() > efSearch) {
                    results.pop();
                }
            }
        }
    }
    visited->reset();
}

void VectorIndexBuilder::shrinkNeighbors(NodeTableDistanceComputer<float>* dc,
    std::priority_queue<NodeDistCloser>& results, int maxSize,
    const std::function<vector_id_t(vector_id_t)>& getActualId) {
    if (results.size() <= maxSize) {
        return;
    }

    std::priority_queue<NodeDistFarther> temp;
    std::vector<NodeDistFarther> result;
    while (!results.empty()) {
        temp.emplace(results.top().id, results.top().dist);
        results.pop();
    }
    auto alpha = header->getConfig().alpha;
    // in the neighbors list. We add it in decreasing order of distance such that farthest nodes are
    // always added.
    while (!temp.empty()) {
        auto nodeA = temp.top();
        temp.pop();
        auto distNodeAQ = nodeA.dist;
        bool good = true;
        for (NodeDistFarther& nodeB : result) {
            double distNodeAB;
            dc->computeDistance(getActualId(nodeA.id), getActualId(nodeB.id), &distNodeAB);
            if ((alpha * distNodeAB) < distNodeAQ) {
                good = false;
                break;
            }
        }
        if (good) {
            result.push_back(nodeA);
            if (result.size() >= maxSize) {
                break;
            }
        }
    }
    for (auto& node : result) {
        results.emplace(node.id, node.dist);
    }
}

void VectorIndexBuilder::acornShrinkNeighbors(std::priority_queue<NodeDistCloser> &results, int maxSize,
                                              const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours) {
    bool isUpperLayer = maxSize == header->getConfig().maxNbrs * header->getConfig().gamma;
    std::priority_queue<NodeDistFarther> input;
    std::vector<NodeDistFarther> output;
    while (!results.empty()) {
        input.emplace(results.top().id, results.top().dist);
        results.pop();
    }

    // if we are in the upper layer, we need to add the neighbors of the neighbors
    if (isUpperLayer) {
        while (!input.empty()) {
            results.emplace(input.top().id, input.top().dist);
            input.pop();
            if (results.size() >= maxSize) {
                break;
            }
        }
    }

    std::unordered_set<vector_id_t> neigh_of_neigh;
    int node_num = 0;

    while (!input.empty()) {
        node_num = node_num + 1;
        NodeDistFarther v1 = input.top();
        input.pop();
        bool good = true;

        if (node_num > header->getConfig().maxNbrsBeta  && neigh_of_neigh.count(v1.id) > 0) {
            good = false;
        }

        if (good) {
            output.push_back(v1);
            if (output.size() >= maxSize) {
                return;
            }

            // update neigh of neigh set
            neigh_of_neigh.insert(v1.id);
            if (node_num > header->getConfig().maxNbrsBeta) {
                size_t begin, end;
                auto neighbors = getNeighbours(v1.id, begin, end);
                for (size_t j = begin; j < end; j++) {
                    if (neighbors[j] < 0) // mod
                        break;
                    neigh_of_neigh.insert(neighbors[j]); // mod
                }
            }

            // break if neigh_of_neigh set is sufficiently large
            if (neigh_of_neigh.size() >= maxSize) {
                break;
            }
        }
    }
    for (auto& node : output) {
        results.emplace(node.id, node.dist);
    }
}

void VectorIndexBuilder::makeConnection(NodeTableDistanceComputer<float>* dc, vector_id_t src, vector_id_t dest,
    double distSrcDest, int maxNbrs,
    const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
    const std::function<vector_id_t(vector_id_t)>& getActualId) {
    size_t begin, end;
    auto neighbors = getNeighbours(src, begin, end);

    // TODO: Optimize this code using bulk shrink. Basically accumulate the neighbors in a vector
    //      and then shrink in a single go.
    if (neighbors[end - 1] == INVALID_VECTOR_ID) {
        if (neighbors[begin] == INVALID_VECTOR_ID) {
            neighbors[begin] = dest;
            return;
        }
        // do loop in reverse order, it could yield faster results
        for (size_t i = end; i > begin; i--) {
            if (neighbors[i - 1] != INVALID_VECTOR_ID) {
                neighbors[i] = dest;
                return;
            }
        }
    }

    // Otherwise we need to shrink the neighbors list
    std::priority_queue<NodeDistCloser> results;
    results.emplace(dest, distSrcDest);
    for (size_t i = begin; i < end; i++) {
        auto neighbor = neighbors[i];
        double distSrcNbr;
        dc->computeDistance(getActualId(src), getActualId(neighbor), &distSrcNbr);
        results.emplace(neighbor, distSrcNbr);
    }
    acornShrinkNeighbors(results, maxNbrs, getNeighbours);
    size_t i = begin;
    while (!results.empty()) {
        neighbors[i++] = results.top().id;
        results.pop();
    }
    while (i < end) {
        neighbors[i++] = INVALID_VECTOR_ID;
    }
}

void VectorIndexBuilder::insertNode(NodeTableDistanceComputer<float>* dc, vector_id_t id, vector_id_t entrypoint,
    double entrypointDist, int maxNbrs, int efConstruction, VisitedTable* visited,
    std::vector<NodeDistCloser>& backNbrs,
    const std::function<vector_id_t*(vector_id_t, size_t&, size_t&)>& getNeighbours,
    const std::function<vector_id_t(vector_id_t)>& getActualId) {
    std::priority_queue<NodeDistCloser> nearestNbrs;
    searchANN(dc, nearestNbrs, entrypoint, entrypointDist, efConstruction, visited, getNeighbours,
        getActualId);
    acornShrinkNeighbors(nearestNbrs, maxNbrs, getNeighbours);

    backNbrs.reserve(nearestNbrs.size());
    while (!nearestNbrs.empty()) {
        auto neighborNode = nearestNbrs.top();
        nearestNbrs.pop();
        if (neighborNode.id == id) {
            continue;
        }
        makeConnection(dc, id, neighborNode.id, neighborNode.dist, maxNbrs, getNeighbours,
            getActualId);
        backNbrs.emplace_back(neighborNode.id, neighborNode.dist);
    }
}

void VectorIndexBuilder::findEntrypointUsingUpperLayer(NodeTableDistanceComputer<float>* dc,
    vector_id_t& entrypoint, double* entrypointDist) {
    // TODO: This should be behind some lock
    uint8_t entrypointLevel;
    partitionHeader->getEntrypoint(entrypoint, entrypointLevel);
    if (entrypointLevel == 1) {
        dc->computeDistance(partitionHeader->getActualId(entrypoint), entrypointDist);
        searchNNOnUpperLevel(dc, entrypoint, *entrypointDist);
        entrypoint = partitionHeader->getActualId(entrypoint);
    } else {
        dc->computeDistance(entrypoint, entrypointDist);
    }
}

void VectorIndexBuilder::insertNodeInUpperLayer(const float* vector, vector_id_t id, VisitedTable* visited,
    NodeTableDistanceComputer<float>* dc) {
    std::vector<NodeDistCloser> backNbrs;
    {
        auto actualId = partitionHeader->getActualId(id);
        std::lock_guard<std::mutex> lock(locks[actualId]);
        dc->setQuery(vector);
        vector_id_t entrypoint;
        uint8_t entrypointLevel;
        partitionHeader->getEntrypoint(entrypoint, entrypointLevel);
        double entrypointDist;
        if (entrypointLevel == 1) {
            dc->computeDistance(partitionHeader->getActualId(entrypoint), &entrypointDist);
        } else {
            entrypoint = id;
            entrypointDist = 0;
        }
        insertNode(
            dc, id, entrypoint, entrypointDist, header->getConfig().maxNbrs * header->getConfig().gamma,
            header->getConfig().efConstruction, visited, backNbrs,
            [&](vector_id_t _id, size_t& begin, size_t& end) {
                return partitionHeader->getNeighbors(_id, begin, end);
            },
            [&](vector_id_t _id) { return partitionHeader->getActualId(_id); });
    }
    // Now update the neighbors of the back neighbors
    for (auto& backNbr : backNbrs) {
        {
            std::lock_guard<std::mutex> lock(locks[partitionHeader->getActualId(backNbr.id)]);
            makeConnection(
                dc, backNbr.id, id, backNbr.dist, header->getConfig().maxNbrs * header->getConfig().gamma,
                [&](vector_id_t _id, size_t& begin, size_t& end) {
                    return partitionHeader->getNeighbors(_id, begin, end);
                },
                [&](vector_id_t _id) { return partitionHeader->getActualId(_id); });
        }
    }
}

void VectorIndexBuilder::insertNodeInLowerLayer(const float* vector, vector_id_t id, VisitedTable* visited,
    NodeTableDistanceComputer<float>* dc) {
    std::vector<NodeDistCloser> backNbrs;
    {
        std::lock_guard<std::mutex> lock(locks[id]);
        dc->setQuery(vector);
        vector_id_t entrypoint;
        double entrypointDist;
        findEntrypointUsingUpperLayer(dc, entrypoint, &entrypointDist);
        insertNode(
            dc, id, entrypoint, entrypointDist, header->getConfig().maxNbrs * header->getConfig().gamma * 2,
            header->getConfig().efConstruction, visited, backNbrs,
            [&](vector_id_t _id, size_t& begin, size_t& end) {
                return graphStorage->getNeighbors(_id, begin, end);
            },
            [&](vector_id_t _id) { return _id; });
    }
    // Now update the neighbors of the back neighbors
    for (auto& backNbr : backNbrs) {
        {
            std::lock_guard<std::mutex> lock(locks[backNbr.id]);
            makeConnection(
                dc, backNbr.id, id, backNbr.dist, header->getConfig().maxNbrs * header->getConfig().gamma * 2,
                [&](vector_id_t _id, size_t& begin, size_t& end) {
                    return graphStorage->getNeighbors(_id, begin, end);
                },
                [&](vector_id_t _id) { return _id; });
        }
    }
}

void VectorIndexBuilder::batchInsert(const float* vectors, const vector_id_t* vectorIds,
    uint64_t numVectors, VisitedTable* visited, NodeTableDistanceComputer<float>* dc) {
    std::vector<vector_id_t> upperLayerVectorIds;
    // first copy the vectors to the temporary storage
    // vectorTempStorage->copyVectors(vectors, vectorIds, numVectors);
    partitionHeader->update(vectorIds, numVectors, upperLayerVectorIds);
//    printf("Ingesting %llu nodes\n", numVectors);
    // Monitoring metrics
//    nodeCounter.fetch_add(numVectors);
//    if (nodeCounter.load() > 10000) {
//        printf("Ingested %llu nodes\n", nodeCounter.load());
//        nodeCounter.store(0);
//    }

    std::unordered_map<vector_id_t, vector_id_t> vectorsInUpperLayer;
    for (auto id : upperLayerVectorIds) {
        vectorsInUpperLayer[partitionHeader->getActualId(id)] = id;
    }

    // Now insert the vectors in the lower layer
    for (uint64_t i = 0; i < numVectors; i++) {
        auto id = vectorIds[i];
        auto vector = vectors + (i * header->getDim());
        partitionHeader->updateEntrypoint(id, 0);
        insertNodeInLowerLayer(vector, id, visited, dc);
        if (vectorsInUpperLayer.contains(id)) {
            insertNodeInUpperLayer(vector, vectorsInUpperLayer[id], visited, dc);
            partitionHeader->updateEntrypoint(vectorsInUpperLayer[id], 1);
        }
    }
}

void VectorIndexBuilder::search(const float* query, int k, int efSearch, VisitedTable* visited,
    std::priority_queue<NodeDistCloser>& results, NodeTableDistanceComputer<float>* dc) {
    dc->setQuery(query);
    vector_id_t entrypoint;
    double entrypointDist;
    findEntrypointUsingUpperLayer(dc, entrypoint, &entrypointDist);
    searchANN(
        dc, results, entrypoint, entrypointDist, efSearch, visited,
        [&](vector_id_t _id, size_t& begin, size_t& end) {
            return graphStorage->getNeighbors(_id, begin, end);
        },
        [&](vector_id_t _id) { return _id; });
}

//void IndexKNN::search(int k, const float* queries, double* distances, vector_id_t* resultIds) {
//    // Find the k nearest neighbors
//    dc->setQuery(queries);
//    std::priority_queue<NodeDistFarther> res;
//    for (int i = 0; i < numEntries; i++) {
//        double d;
//        dc->computeDistance(i, &d);
//        res.emplace(i, d);
//    }
//    for (int i = 0; i < k; i++) {
//        resultIds[i] = res.top().id;
//        distances[i] = res.top().dist;
//        res.pop();
//    }
//}

} // namespace storage
} // namespace kuzu