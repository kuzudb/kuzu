#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "common/constants.h"
#include "common/types/internal_id_t.h"
#include "processor/data_pos.h"
#include "processor/operator/copy_from/read_rdf.h"
#include "processor/operator/sink.h"
#include "storage/copier/rel_copy_executor.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/store/nodes_store.h"
#include "storage/store/rel_table.h"
#include <_types/_uint32_t.h>

namespace kuzu {
namespace processor {

class CopyRDFSharedState {
public:
    CopyRDFSharedState(common::rdf_graph_id_t rdfGraphId, storage::NodeTable* resourcesNodeTable,
        storage::RelTable* triplesRelTable, storage::RelsStore* relsStore,
        catalog::Catalog* catalog, storage::WAL* wal, storage::MemoryManager* memoryManager,
        std::shared_ptr<ReadRDFSharedState> readerSharedState);

    inline void initializeNodeGroup(common::CopyDescription* copyDesc) {
        nodeGroup = std::make_unique<storage::NodeGroup>(resourcesNodeTableSchema, copyDesc);
    }

    inline void initializePrimaryKey();

    inline common::offset_t getNextNodeGroupIdx() { return currentNodeGroupIdx++; }

    inline void logCopyNodeWALRecord();

    inline bool isCopyAllowed() {
        auto nodesStatistics = resourcesNodeTable->getNodeStatisticsAndDeletedIDs();
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(resourcesNodeTable->getTableID())
                   ->getNumTuples() == 0;
    }

public:
    const common::rdf_graph_id_t rdfGraphId;
    const catalog::Catalog* catalog;
    catalog::NodeTableSchema* resourcesNodeTableSchema;
    catalog::RelTableSchema* triplesRelTableSchema;
    storage::NodeTable* resourcesNodeTable;
    storage::RelTable* triplesRelTable;
    storage::RelsStore* relsStore;
    storage::WAL* wal;
    std::mutex mtx;
    std::unique_ptr<storage::HashIndexBuilder<common::ku_string_t>> pkHashIndex;
    std::shared_ptr<FactorizedTable> fTable;
    common::offset_t numOfTriples;
    common::offset_t currentOffset;
    bool hasInitialized;
    std::unique_ptr<storage::DirectedInMemRelLists> fwdRelData;
    std::unique_ptr<storage::DirectedInMemRelLists> bwdRelData;
    std::unique_ptr<common::SelectionVector> subjectSelectionVector;
    std::unique_ptr<common::SelectionVector> predicateSelectionVector;
    std::unique_ptr<common::SelectionVector> objectSelectionVector;
    std::shared_ptr<ReadRDFSharedState> readerSharedState;
    uint64_t currentNodeGroupIdx;
    std::unique_ptr<storage::NodeGroup> nodeGroup;
};

struct CopyRDFLocalState {
    CopyRDFLocalState(const common::CopyDescription& copyDesc, const DataPos& subjectVectorPos,
        const DataPos& predicateVectorPos, const DataPos& objectVectorPos,
        const DataPos& offsetVectorPos)
        : copyDesc{copyDesc}, subjectVectorPos{subjectVectorPos},
          predicateVectorPos{predicateVectorPos}, objectVectorPos{objectVectorPos},
          offsetVectorPos{offsetVectorPos}, subjectOffsets{std::vector<offset_t>(
                                                common::DEFAULT_VECTOR_CAPACITY)},
          predicateOffsets{std::vector<offset_t>(common::DEFAULT_VECTOR_CAPACITY)},
          objectOffsets{std::vector<offset_t>(common::DEFAULT_VECTOR_CAPACITY)} {}

    common::CopyDescription copyDesc;
    DataPos subjectVectorPos;
    DataPos predicateVectorPos;
    DataPos objectVectorPos;
    DataPos offsetVectorPos;
    std::shared_ptr<common::ValueVector> subjectVector;
    std::shared_ptr<common::ValueVector> predicateVector;
    std::shared_ptr<common::ValueVector> objectVector;
    std::shared_ptr<common::ValueVector> offsetVector;
    std::vector<common::offset_t> subjectOffsets;
    std::vector<common::offset_t> predicateOffsets;
    std::vector<common::offset_t> objectOffsets;
};

class CopyRDF : public Sink {
public:
    CopyRDF(std::unique_ptr<CopyRDFLocalState> localState,
        std::shared_ptr<CopyRDFSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_RDF, std::move(child), id,
              paramsString},
          localState{std::move(localState)}, sharedState{std::move(sharedState)} {}

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        localState->offsetVector = resultSet->getValueVector(localState->offsetVectorPos);
        localState->subjectVector = resultSet->getValueVector(localState->subjectVectorPos);
        localState->predicateVector = resultSet->getValueVector(localState->predicateVectorPos);
        localState->objectVector = resultSet->getValueVector(localState->objectVectorPos);
    }

    inline void initGlobalStateInternal(ExecutionContext* context) override {
        if (!sharedState->isCopyAllowed()) {
            throw common::CopyException("COPY command can only be executed once on a RDF Graph.");
        }
        sharedState->initializePrimaryKey();
        sharedState->initializeNodeGroup(&localState->copyDesc);
    }

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyRDF>(std::make_unique<CopyRDFLocalState>(*localState),
            sharedState, resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

private:
    void copyNodeTableAndInitializeRelLists(kuzu::processor::ExecutionContext* context);

    void populateHashIndexAndColumnChunk(const common::offset_t& length);

    inline void lookupHashIndex(
        const std::shared_ptr<common::ValueVector>& vector, int pos, common::offset_t& offset);

    inline void appendToHashIndex(common::ValueVector* vector, const common::offset_t& length,
        std::vector<offset_t>& offsets, SelectionVector* selectionVector);

    inline void lookupAndBuildOffset(const common::offset_t& length);

    void copyRelLists(const common::offset_t& startOffset, const common::offset_t& length,
        storage::DirectedInMemRelLists* directedInMemRelLists,
        const std::vector<offset_t>& boundOffsets, const std::vector<offset_t>& adjOffsets,
        const std::vector<offset_t>& propertyValues);

    inline void copyToNodeGroup(common::ValueVector* vector, SelectionVector* selectionVector);

    void initializeDirectedInMemRelData(common::RelDataDirection direction,
        const std::unique_ptr<storage::DirectedInMemRelLists>& relLists);

    void countRelListsSize(std::vector<std::uint64_t>& fwdRelListSizes,
        std::vector<std::uint64_t>& bwdRelListSizes, const common::offset_t& length);

    void buildRelListsHeaders(storage::ListHeadersBuilder* relListHeadersBuilder,
        const storage::atomic_uint64_vec_t& relListsSizes);

    void buildRelListsMetadata(storage::DirectedInMemRelLists* directedInMemRelLists);

    void buildRelListsMetadata(
        storage::InMemLists* relLists, storage::ListHeadersBuilder* relListHeaders);

    static void writeAndResetNodeGroup(common::node_group_idx_t nodeGroupIdx,
        storage::NodeTable* table, storage::NodeGroup* nodeGroup);

private:
    std::unique_ptr<CopyRDFLocalState> localState;
    std::shared_ptr<CopyRDFSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
