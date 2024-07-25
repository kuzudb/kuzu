#include <queue>

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/value/nested.h"
#include "common/vector_index/distance_computer.h"
#include "common/vector_index/helpers.h"
#include "function/gds/gds.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"
#include "storage/index/vector_index_header.h"
#include "storage/storage_manager.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct VectorSearchBindData final : public GDSBindData {
    table_id_t nodeTableID = INVALID_TABLE_ID;
    property_id_t embeddingPropertyID = INVALID_PROPERTY_ID;
    int k = 100;
    int efSearch = 100;
    std::vector<float> queryVector;

    VectorSearchBindData(std::shared_ptr<binder::Expression> nodeOutput, table_id_t nodeTableID,
        property_id_t embeddingPropertyID, int k, int64_t efSearch, std::vector<float> queryVector,
        bool outputAsNode)
        : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeTableID(nodeTableID),
          embeddingPropertyID(embeddingPropertyID), k(k), efSearch(efSearch),
          queryVector(std::move(queryVector)){};

    VectorSearchBindData(const VectorSearchBindData& other)
        : GDSBindData{other}, nodeTableID(other.nodeTableID),
          embeddingPropertyID(other.embeddingPropertyID), k(other.k), efSearch{other.efSearch},
          queryVector(other.queryVector) {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<VectorSearchBindData>(*this);
    }
};

class VectorSearchLocalState : public GDSLocalState {
public:
    explicit VectorSearchLocalState(main::ClientContext* context, table_id_t nodeTableId,
        property_id_t embeddingPropertyId) {
        auto mm = context->getMemoryManager();
        nodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        distanceVector = std::make_unique<ValueVector>(LogicalType::DOUBLE(), mm);
        nodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
        distanceVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(nodeIdVector.get());
        vectors.push_back(distanceVector.get());

        indexHeader = context->getStorageManager()->getVectorIndexHeaderReadOnlyVersion(nodeTableId,
            embeddingPropertyId);
        auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(
            context->getStorageManager()->getTable(nodeTableId));
        embeddingColumn = nodeTable->getColumn(embeddingPropertyId);

        // TODO: Replace with compressed vector. Should be stored in the
        embeddingVector = std::make_unique<ValueVector>(embeddingColumn->getDataType().copy(),
            context->getMemoryManager());
        embeddingVector->state = DataChunkState::getSingleValueDataChunkState();
        inputNodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID().copy(),
            context->getMemoryManager());
        inputNodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
        for (size_t i = 0; i < embeddingColumn->getNumNodeGroups(context->getTx()); i++) {
            readStates.push_back(std::make_unique<Column::ChunkState>());
        }
    }

    void materialize(graph::Graph* graph, std::priority_queue<NodeDistCloser>& result,
        FactorizedTable& table, int k) const {
        // Remove elements until we have k elements
        while (result.size() > k) {
            result.pop();
        }

        std::priority_queue<NodeDistFarther> reversed;
        while (!result.empty()) {
            reversed.emplace(result.top().id, result.top().dist);
            result.pop();
        }

        while (!reversed.empty()) {
            auto res = reversed.top();
            reversed.pop();
            auto nodeID = nodeID_t{res.id, indexHeader->getNodeTableId()};
            nodeIdVector->setValue<nodeID_t>(0, nodeID);
            distanceVector->setValue<double>(0, res.dist);
            table.append(vectors);
        }
    }

public:
    std::unique_ptr<ValueVector> nodeIdVector;
    std::unique_ptr<ValueVector> distanceVector;
    std::vector<ValueVector*> vectors;

    VectorIndexHeader* indexHeader;
    Column* embeddingColumn;
    std::unique_ptr<ValueVector> embeddingVector;
    std::unique_ptr<ValueVector> inputNodeIdVector;
    std::vector<std::unique_ptr<Column::ChunkState>> readStates;
};

class VectorSearch : public GDSAlgorithm {
    static constexpr char DISTANCE_COLUMN_NAME[] = "_distance";

public:
    VectorSearch() = default;
    VectorSearch(const VectorSearch& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * embeddingProperty::ANY
     * efSearch::INT64
     * outputProperty::BOOL
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::INT64, LogicalTypeID::LIST, LogicalTypeID::INT64,
            LogicalTypeID::INT64, LogicalTypeID::BOOL};
    }

    /*
     * Outputs are
     *
     * nodeID::INTERNAL_ID
     * distance::DOUBLE
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(binder->createVariable(DISTANCE_COLUMN_NAME, LogicalType::DOUBLE()));
        return columns;
    }

    void bind(const expression_vector& params, Binder* binder, GraphEntry& graphEntry) override {
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        auto nodeTableId = graphEntry.nodeTableIDs[0];
        auto embeddingPropertyId = ExpressionUtil::getLiteralValue<int64_t>(*params[1]);
        auto val = params[2]->constCast<LiteralExpression>().getValue();
//        KU_ASSERT(val.getDataType().getLogicalTypeID() == LogicalTypeID::LIST);
//        auto childSize = NestedVal::getChildrenSize(&val);
//        std::vector<float> queryVector(960);
//        for (size_t i = 0; i < childSize; i++) {
//            auto child = NestedVal::getChildVal(&val, i);
//            KU_ASSERT(child->getDataType().getLogicalTypeID() == LogicalTypeID::DOUBLE);
//            queryVector[i] = child->getValue<double>();
//        }

        std::vector<float> queryVector = {1.0,3.0,11.0,110.0,62.0,22.0,4.0,0.0,43.0,21.0,22.0,18.0,6.0,28.0,64.0,9.0,11.0,1.0,0.0,0.0,1.0,40.0,101.0,21.0,20.0,2.0,4.0,2.0,2.0,9.0,18.0,35.0,1.0,1.0,7.0,25.0,108.0,116.0,63.0,2.0,0.0,0.0,11.0,74.0,40.0,101.0,116.0,3.0,33.0,1.0,1.0,11.0,14.0,18.0,116.0,116.0,68.0,12.0,5.0,4.0,2.0,2.0,9.0,102.0,17.0,3.0,10.0,18.0,8.0,15.0,67.0,63.0,15.0,0.0,14.0,116.0,80.0,0.0,2.0,22.0,96.0,37.0,28.0,88.0,43.0,1.0,4.0,18.0,116.0,51.0,5.0,11.0,32.0,14.0,8.0,23.0,44.0,17.0,12.0,9.0,0.0,0.0,19.0,37.0,85.0,18.0,16.0,104.0,22.0,6.0,2.0,26.0,12.0,58.0,67.0,82.0,25.0,12.0,2.0,2.0,25.0,18.0,8.0,2.0,19.0,42.0,48.0,11.0};
        auto k = ExpressionUtil::getLiteralValue<int64_t>(*params[3]);
        auto efSearch = ExpressionUtil::getLiteralValue<int64_t>(*params[4]);
        auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[5]);
        bindData = std::make_unique<VectorSearchBindData>(nodeOutput, nodeTableId,
            embeddingPropertyId, k, efSearch, queryVector, outputProperty);
    }

    void initLocalState(main::ClientContext* context) override {
        auto bind = ku_dynamic_cast<GDSBindData*, VectorSearchBindData*>(bindData.get());
        localState = std::make_unique<VectorSearchLocalState>(context, bind->nodeTableID,
            bind->embeddingPropertyID);
    }

    void searchNNOnUpperLevel(ExecutionContext* context, VectorIndexHeader* header,
        DistanceComputer* dc, vector_id_t& nearest, double& nearestDist) {
        while (true) {
            vector_id_t prev_nearest = nearest;
            size_t begin, end;
            auto neighbors = header->getNeighbors(nearest, begin, end);
            for (size_t i = begin; i < end; i++) {
                vector_id_t neighbor = neighbors[i];
                if (neighbor == INVALID_VECTOR_ID) {
                    break;
                }
                double dist;
                auto embed = getEmbedding(context, header->getActualId(neighbor));
                dc->computeDistance(embed, &dist);
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

    // TODO: Use in-mem compressed vectors
    // TODO: Maybe try using separate threads to separate io and computation (ideal async io)
    const float* getEmbedding(processor::ExecutionContext* context, vector_id_t id) {
        auto searchLocalState =
            ku_dynamic_cast<GDSLocalState*, VectorSearchLocalState*>(localState.get());
        auto embeddingColumn = searchLocalState->embeddingColumn;
        auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

        // Initialize the read state
        auto readState = searchLocalState->readStates[nodeGroupIdx].get();
        embeddingColumn->initChunkState(context->clientContext->getTx(), nodeGroupIdx, *readState);

        // Read the embedding
        auto nodeIdVector = searchLocalState->inputNodeIdVector.get();
        nodeIdVector->setValue(0, id);
        auto resultVector = searchLocalState->embeddingVector.get();
        embeddingColumn->lookup(context->clientContext->getTx(), *readState, nodeIdVector,
            resultVector);
        return reinterpret_cast<float*>(ListVector::getDataVector(resultVector)->getData());
    }

    void findEntrypointUsingUpperLayer(ExecutionContext* context, VectorIndexHeader* header,
        DistanceComputer* dc, vector_id_t& entrypoint, double* entrypointDist) {
        uint8_t entrypointLevel;
        header->getEntrypoint(entrypoint, entrypointLevel);
        if (entrypointLevel == 1) {
            auto embedding = getEmbedding(context, header->getActualId(entrypoint));
            dc->computeDistance(embedding, entrypointDist);
            searchNNOnUpperLevel(context, header, dc, entrypoint, *entrypointDist);
            entrypoint = header->getActualId(entrypoint);
        } else {
            dc->computeDistance(entrypoint, entrypointDist);
        }
    }

    void exec(ExecutionContext* context) override {
        auto searchLocalState =
            ku_dynamic_cast<GDSLocalState*, VectorSearchLocalState*>(localState.get());
        auto bindState = ku_dynamic_cast<GDSBindData*, VectorSearchBindData*>(bindData.get());
        auto header = searchLocalState->indexHeader;
        auto graph = sharedState->graph.get();
        auto nodeTableId = header->getNodeTableId();
        auto efSearch = bindState->efSearch;
        auto k = bindState->k;
        KU_ASSERT(bindState->queryVector.size() == header->getDim());
        auto query = bindState->queryVector.data();
        L2DistanceComputer dc(nullptr, header->getDim(), 0);
        dc.setQuery(query);
        // Todo: Use bitset here
        auto visited = std::make_unique<VisitedTable>(header->getNumVectors());
        vector_id_t entrypoint;
        double entrypointDist;
        findEntrypointUsingUpperLayer(context, header, &dc, entrypoint, &entrypointDist);

        std::priority_queue<NodeDistFarther> candidates;
        std::priority_queue<NodeDistCloser> results;
        candidates.emplace(entrypoint, entrypointDist);
        results.emplace(entrypoint, entrypointDist);
        visited->set(entrypoint);
        while (!candidates.empty()) {
            auto candidate = candidates.top();
            if (candidate.dist > results.top().dist) {
                break;
            }
            candidates.pop();
            auto neighbors = graph->scanFwd({candidate.id, nodeTableId});
            // TODO: Try batch compute distances
            for (auto neighbor : neighbors) {
                if (visited->get(neighbor.offset)) {
                    continue;
                }
                visited->set(neighbor.offset);
                double dist;
                auto embed = getEmbedding(context, neighbor.offset);
                dc.computeDistance(embed, &dist);
                if (results.size() < efSearch || dist < results.top().dist) {
                    candidates.emplace(neighbor.offset, dist);
                    results.emplace(neighbor.offset, dist);
                    if (results.size() > efSearch) {
                        results.pop();
                    }
                }
            }
        }
        // Materialize the results
        searchLocalState->materialize(graph, results, *sharedState->fTable, k);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<VectorSearch>(*this);
    }
};

function_set VectorSearchFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<VectorSearch>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
