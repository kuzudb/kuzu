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

struct CopyNodeDataInfo {
    DataPos rowIdxVectorPos;
    DataPos filePathVectorPos;
    std::vector<DataPos> dataColumnPoses;
};

class CopyNode : public Sink {
public:
    CopyNode(std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeDataInfo copyNodeDataInfo,
        const common::CopyDescription& copyDesc, storage::NodeTable* table,
        storage::RelsStore* relsStore, catalog::Catalog* catalog, storage::WAL* wal,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_NODE, std::move(child),
              id, paramsString},
          sharedState{std::move(sharedState)}, copyNodeDataInfo{std::move(copyNodeDataInfo)},
          copyDesc{copyDesc}, table{table}, relsStore{relsStore}, catalog{catalog}, wal{wal},
          rowIdxVector{nullptr}, filePathVector{nullptr} {
        auto tableSchema = catalog->getReadOnlyVersion()->getNodeTableSchema(table->getTableID());
        copyStates.resize(tableSchema->getNumProperties());
        for (auto i = 0u; i < tableSchema->getNumProperties(); i++) {
            auto& property = tableSchema->properties[i];
            copyStates[i] = std::make_unique<storage::PropertyCopyState>(property.dataType);
        }
    }

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        rowIdxVector = resultSet->getValueVector(copyNodeDataInfo.rowIdxVectorPos).get();
        filePathVector = resultSet->getValueVector(copyNodeDataInfo.filePathVectorPos).get();
        for (auto& arrowColumnPos : copyNodeDataInfo.dataColumnPoses) {
            dataColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
        }
    }

    inline void initGlobalStateInternal(ExecutionContext* context) override {
        if (!isCopyAllowed()) {
            throw common::CopyException("COPY commands can only be executed once on a table.");
        }
        auto nodeTableSchema =
            catalog->getReadOnlyVersion()->getNodeTableSchema(table->getTableID());
        sharedState->initialize(nodeTableSchema, wal->getDirectory());
    }

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNode>(sharedState, copyNodeDataInfo, copyDesc, table, relsStore,
            catalog, wal, resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

protected:
    void populatePKIndex(storage::InMemColumnChunk* chunk, storage::InMemOverflowFile* overflowFile,
        common::offset_t startOffset, uint64_t numValues, const std::string& filePath,
        common::row_idx_t startRowIdxInFile);

    void logCopyWALRecord();

    std::pair<common::row_idx_t, common::row_idx_t> getStartAndEndRowIdx(
        common::vector_idx_t columnIdx);
    std::pair<std::string, common::row_idx_t> getFilePathAndRowIdxInFile();

private:
    inline bool isCopyAllowed() {
        auto nodesStatistics = table->getNodeStatisticsAndDeletedIDs();
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(table->getTableID())
                   ->getNumTuples() == 0;
    }

    void flushChunksAndPopulatePKIndex(
        const std::vector<std::unique_ptr<storage::InMemColumnChunk>>& columnChunks,
        common::offset_t startNodeOffset, common::offset_t endNodeOffset,
        const std::string& filePath, common::row_idx_t startRowIdxInFile);

    template<typename T, typename... Args>
    uint64_t appendToPKIndex(storage::InMemColumnChunk* chunk, common::offset_t startOffset,
        uint64_t numValues, Args... args) {
        throw common::CopyException("appendToPKIndex1 not implemented");
    }

protected:
    std::shared_ptr<CopyNodeSharedState> sharedState;
    CopyNodeDataInfo copyNodeDataInfo;
    common::CopyDescription copyDesc;
    storage::NodeTable* table;
    storage::RelsStore* relsStore;
    catalog::Catalog* catalog;
    storage::WAL* wal;
    common::ValueVector* rowIdxVector;
    common::ValueVector* filePathVector;
    std::vector<common::ValueVector*> dataColumnVectors;
    std::vector<std::unique_ptr<storage::PropertyCopyState>> copyStates;
};

} // namespace processor
} // namespace kuzu
