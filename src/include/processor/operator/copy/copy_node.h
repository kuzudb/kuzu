#pragma once

#include "processor/operator/sink.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class CopyNodeSharedState {
public:
    CopyNodeSharedState(uint64_t& numRows, storage::MemoryManager* memoryManager);

    inline void initialize(
        catalog::NodeTableSchema* nodeTableSchema, const std::string& directory) {
        initializePrimaryKey(nodeTableSchema, directory);
        initializeColumns(nodeTableSchema, directory);
    };

private:
    void initializePrimaryKey(
        catalog::NodeTableSchema* nodeTableSchema, const std::string& directory);

    void initializeColumns(catalog::NodeTableSchema* nodeTableSchema, const std::string& directory);

public:
    common::column_id_t pkColumnID;
    std::vector<std::unique_ptr<storage::InMemColumn>> columns;
    std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex;
    uint64_t& numRows;
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> table;
    bool hasLoggedWAL;
};

struct CopyNodeLocalState {
    CopyNodeLocalState(common::CopyDescription copyDesc, storage::NodeTable* table,
        storage::RelsStore* relsStore, catalog::Catalog* catalog, storage::WAL* wal,
        DataPos offsetVectorPos, std::vector<DataPos> arrowColumnPoses)
        : copyDesc{std::move(copyDesc)}, table{table}, relsStore{relsStore}, catalog{catalog},
          wal{wal}, offsetVectorPos{std::move(offsetVectorPos)}, arrowColumnPoses{
                                                                     std::move(arrowColumnPoses)} {}

    common::CopyDescription copyDesc;
    storage::NodeTable* table;
    storage::RelsStore* relsStore;
    catalog::Catalog* catalog;
    storage::WAL* wal;
    DataPos offsetVectorPos;
    common::ValueVector* offsetVector;
    std::vector<DataPos> arrowColumnPoses;
    std::vector<common::ValueVector*> arrowColumnVectors;
};

class CopyNode : public Sink {
public:
    CopyNode(std::unique_ptr<CopyNodeLocalState> localState,
        std::shared_ptr<CopyNodeSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_NODE, std::move(child),
              id, paramsString},
          localState{std::move(localState)}, sharedState{std::move(sharedState)} {}

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        localState->offsetVector = resultSet->getValueVector(localState->offsetVectorPos).get();
        for (auto& arrowColumnPos : localState->arrowColumnPoses) {
            localState->arrowColumnVectors.push_back(
                resultSet->getValueVector(arrowColumnPos).get());
        }
    }

    inline void initGlobalStateInternal(ExecutionContext* context) override {
        if (!isCopyAllowed()) {
            throw common::CopyException("COPY commands can only be executed once on a table.");
        }
        auto nodeTableSchema = localState->catalog->getReadOnlyVersion()->getNodeTableSchema(
            localState->table->getTableID());
        sharedState->initialize(nodeTableSchema, localState->wal->getDirectory());
    }

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNode>(std::make_unique<CopyNodeLocalState>(*localState),
            sharedState, resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

private:
    inline bool isCopyAllowed() {
        auto nodesStatistics = localState->table->getNodeStatisticsAndDeletedIDs();
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(localState->table->getTableID())
                   ->getNumTuples() == 0;
    }

    void flushChunksAndPopulatePKIndex(
        const std::vector<std::unique_ptr<storage::InMemColumnChunk>>& columnChunks,
        common::offset_t startNodeOffset, common::offset_t endNodeOffset);

    template<typename T, typename... Args>
    void appendToPKIndex(storage::InMemColumnChunk* chunk, common::offset_t startOffset,
        uint64_t numValues, Args... args) {
        throw common::CopyException("appendToPKIndex1 not implemented");
    }

    void populatePKIndex(storage::InMemColumnChunk* chunk, storage::InMemOverflowFile* overflowFile,
        common::offset_t startOffset, uint64_t numValues);

private:
    std::unique_ptr<CopyNodeLocalState> localState;
    std::shared_ptr<CopyNodeSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
