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

struct CopyNodeInfo {
    DataPos rowIdxVectorPos;
    DataPos filePathVectorPos;
    std::vector<DataPos> dataColumnPoses;
    common::CopyDescription copyDesc;
    storage::NodeTable* table;
    storage::RelsStore* relsStore;
    catalog::Catalog* catalog;
    storage::WAL* wal;
};

class CopyNode : public Sink {
public:
    CopyNode(std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeInfo copyNodeInfo,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString);

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        rowIdxVector = resultSet->getValueVector(copyNodeInfo.rowIdxVectorPos).get();
        filePathVector = resultSet->getValueVector(copyNodeInfo.filePathVectorPos).get();
        for (auto& arrowColumnPos : copyNodeInfo.dataColumnPoses) {
            dataColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
        }
    }

    inline void initGlobalStateInternal(ExecutionContext* context) override {
        if (!isCopyAllowed()) {
            throw common::CopyException("COPY commands can only be executed once on a table.");
        }
        auto nodeTableSchema = copyNodeInfo.catalog->getReadOnlyVersion()->getNodeTableSchema(
            copyNodeInfo.table->getTableID());
        sharedState->initialize(nodeTableSchema, copyNodeInfo.wal->getDirectory());
    }

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyNode>(sharedState, copyNodeInfo, resultSetDescriptor->copy(),
            children[0]->clone(), id, paramsString);
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
    inline bool isCopyAllowed() const {
        auto nodesStatistics = copyNodeInfo.table->getNodeStatisticsAndDeletedIDs();
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(copyNodeInfo.table->getTableID())
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
    CopyNodeInfo copyNodeInfo;
    common::ValueVector* rowIdxVector;
    common::ValueVector* filePathVector;
    std::vector<common::ValueVector*> dataColumnVectors;
    std::vector<std::unique_ptr<storage::PropertyCopyState>> copyStates;
};

} // namespace processor
} // namespace kuzu
