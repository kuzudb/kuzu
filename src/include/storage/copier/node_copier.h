#pragma once

#include "storage/copier/npy_reader.h"
#include "storage/copier/table_copy_executor.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/node_table.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace kuzu {
namespace storage {

class NodeCopyMorsel {
public:
    static constexpr common::block_idx_t BLOCK_IDX_INVALID = UINT64_MAX;

    NodeCopyMorsel(common::offset_t nodeOffset, common::block_idx_t blockIdx, uint64_t numNodes,
        std::string filePath)
        : nodeOffset{nodeOffset}, blockIdx{blockIdx}, numNodes{numNodes}, filePath{std::move(
                                                                              filePath)} {};
    virtual ~NodeCopyMorsel() = default;

public:
    common::offset_t nodeOffset;
    common::block_idx_t blockIdx;
    uint64_t numNodes;
    std::string filePath;
};

// For CSV file, we need to read in streaming mode, so we need to read one batch at a time.
class CSVNodeCopyMorsel : public NodeCopyMorsel {
public:
    CSVNodeCopyMorsel(common::offset_t startOffset, std::string filePath,
        std::shared_ptr<arrow::RecordBatch> recordBatch)
        : NodeCopyMorsel{startOffset, BLOCK_IDX_INVALID, UINT64_MAX, std::move(filePath)},
          recordBatch{std::move(recordBatch)} {}

    std::shared_ptr<arrow::RecordBatch> recordBatch;
};

class NodeCopySharedState {
public:
    NodeCopySharedState(std::vector<std::string> filePaths,
        std::unordered_map<std::string, TableCopyExecutor::FileBlockInfo> fileBlockInfos)
        : filePaths{std::move(filePaths)}, fileIdx{0}, blockIdx{0}, nodeOffset{0},
          fileBlockInfos{std::move(fileBlockInfos)} {};
    virtual ~NodeCopySharedState() = default;

    virtual std::unique_ptr<NodeCopyMorsel> getMorsel();

public:
    std::vector<std::string> filePaths;
    common::vector_idx_t fileIdx;
    common::offset_t nodeOffset;

protected:
    std::unordered_map<std::string, TableCopyExecutor::FileBlockInfo> fileBlockInfos;
    common::block_idx_t blockIdx;
    std::mutex mtx;
};

// For CSV file, we need to read in streaming mode, so we need to keep the reader in the shared
// state.
class CSVNodeCopySharedState : public NodeCopySharedState {
public:
    CSVNodeCopySharedState(std::vector<std::string> filePaths,
        std::unordered_map<std::string, TableCopyExecutor::FileBlockInfo> fileBlockInfos,
        common::CSVReaderConfig* csvReaderConfig, catalog::TableSchema* tableSchema)
        : NodeCopySharedState{std::move(filePaths), std::move(fileBlockInfos)},
          csvReaderConfig{csvReaderConfig}, tableSchema{tableSchema} {};

    std::unique_ptr<NodeCopyMorsel> getMorsel() override;

private:
    common::CSVReaderConfig* csvReaderConfig;
    catalog::TableSchema* tableSchema;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
};

class NodeCopier {
public:
    NodeCopier(const std::string& directory, std::shared_ptr<NodeCopySharedState> sharedState,
        PrimaryKeyIndexBuilder* pkIndex, const common::CopyDescription& copyDesc,
        catalog::TableSchema* schema, NodeTable* table, common::column_id_t pkColumnID);

    // For clone.
    NodeCopier(std::shared_ptr<NodeCopySharedState> sharedState, PrimaryKeyIndexBuilder* pkIndex,
        const common::CopyDescription& copyDesc, NodeTable* table, common::property_id_t pkColumnID,
        std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties)
        : sharedState{std::move(sharedState)}, pkIndex{pkIndex}, copyDesc{copyDesc}, table{table},
          pkColumnID{pkColumnID}, columns{std::move(columns)}, properties{std::move(properties)} {
        overflowCursors.resize(this->properties.size());
    }

    virtual ~NodeCopier() = default;

    void execute(processor::ExecutionContext* executionContext);

    inline virtual void finalize() {
        if (pkIndex) {
            pkIndex->flush();
        }
        for (auto& column : columns) {
            column->saveToFile();
        }
    }

    virtual std::unique_ptr<NodeCopier> clone() const {
        return std::make_unique<NodeCopier>(
            sharedState, pkIndex, copyDesc, table, pkColumnID, columns, properties);
    }

protected:
    virtual void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
        throw common::CopyException("executeInternal not implemented");
    }

    void copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk, common::column_id_t columnID,
        arrow::Array& arrowArray, common::CopyDescription& copyDescription);
    void populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::offset_t startOffset, uint64_t numValues);

    void flushChunksAndPopulatePKIndex(
        const std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks,
        common::offset_t startNodeOffset, common::offset_t endNodeOffset,
        common::column_id_t columnToFlush = common::INVALID_COLUMN_ID);

    template<typename T, typename... Args>
    void appendToPKIndex(
        InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues, Args... args) {
        throw common::CopyException("appendToPKIndex not implemented");
    }

protected:
    std::shared_ptr<NodeCopySharedState> sharedState;
    PrimaryKeyIndexBuilder* pkIndex;
    common::CopyDescription copyDesc;
    NodeTable* table;
    // The properties to be copied into. Each property corresponds to a column.
    std::vector<catalog::Property> properties;
    std::vector<std::shared_ptr<InMemColumn>> columns;
    std::vector<PageByteCursor> overflowCursors;
    common::column_id_t pkColumnID;
};

template<>
void NodeCopier::appendToPKIndex<int64_t>(
    kuzu::storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues);
template<>
void NodeCopier::appendToPKIndex<common::ku_string_t, storage::InMemOverflowFile*>(
    kuzu::storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues,
    storage::InMemOverflowFile* overflowFile);

class CSVNodeCopier : public NodeCopier {
public:
    CSVNodeCopier(const std::string& directory, std::shared_ptr<NodeCopySharedState> sharedState,
        PrimaryKeyIndexBuilder* pkIndex, const common::CopyDescription& copyDesc,
        catalog::TableSchema* schema, NodeTable* table, common::property_id_t pkColumnID)
        : NodeCopier{
              directory, std::move(sharedState), pkIndex, copyDesc, schema, table, pkColumnID} {}

    // For clone.
    CSVNodeCopier(std::shared_ptr<NodeCopySharedState> sharedState, PrimaryKeyIndexBuilder* pkIndex,
        const common::CopyDescription& copyDesc, NodeTable* table, common::column_id_t pkColumnID,
        std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties)
        : NodeCopier{std::move(sharedState), pkIndex, copyDesc, table, pkColumnID,
              std::move(columns), std::move(properties)} {}

    inline std::unique_ptr<NodeCopier> clone() const override {
        return std::make_unique<CSVNodeCopier>(
            sharedState, pkIndex, copyDesc, table, pkColumnID, columns, properties);
    }

protected:
    void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) override;
};

class ParquetNodeCopier : public NodeCopier {
public:
    ParquetNodeCopier(const std::string& directory,
        std::shared_ptr<NodeCopySharedState> sharedState, PrimaryKeyIndexBuilder* pkIndex,
        const common::CopyDescription& copyDesc, catalog::TableSchema* schema, NodeTable* table,
        common::column_id_t pkColumnID)
        : NodeCopier{
              directory, std::move(sharedState), pkIndex, copyDesc, schema, table, pkColumnID} {}

    // Clone.
    ParquetNodeCopier(std::shared_ptr<NodeCopySharedState> sharedState,
        PrimaryKeyIndexBuilder* pkIndex, const common::CopyDescription& copyDesc, NodeTable* table,
        common::column_id_t pkColumnID, std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties)
        : NodeCopier{std::move(sharedState), pkIndex, copyDesc, table, pkColumnID,
              std::move(columns), std::move(properties)} {}

    inline std::unique_ptr<NodeCopier> clone() const override {
        return std::make_unique<ParquetNodeCopier>(
            sharedState, pkIndex, copyDesc, table, pkColumnID, columns, properties);
    }

protected:
    void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) override;

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

class NPYNodeCopier : public NodeCopier {
public:
    NPYNodeCopier(const std::string& directory, std::shared_ptr<NodeCopySharedState> sharedState,
        PrimaryKeyIndexBuilder* pkIndex, const common::CopyDescription& copyDesc,
        catalog::TableSchema* schema, NodeTable* table, common::column_id_t pkColumnID,
        common::column_id_t columnToCopy)
        : NodeCopier{directory, std::move(sharedState), pkIndex, copyDesc, schema, table,
              pkColumnID},
          columnToCopy{columnToCopy} {}

    // Clone.
    NPYNodeCopier(std::shared_ptr<NodeCopySharedState> sharedState, PrimaryKeyIndexBuilder* pkIndex,
        const common::CopyDescription& copyDesc, NodeTable* table, common::column_id_t pkColumnID,
        common::column_id_t columnToCopy, std::vector<std::shared_ptr<InMemColumn>> columns,
        std::vector<catalog::Property> properties)
        : NodeCopier{std::move(sharedState), pkIndex, copyDesc, table, pkColumnID,
              std::move(columns), std::move(properties)},
          columnToCopy{columnToCopy} {}

    inline std::unique_ptr<NodeCopier> clone() const override {
        return std::make_unique<NPYNodeCopier>(
            sharedState, pkIndex, copyDesc, table, pkColumnID, columnToCopy, columns, properties);
    }

protected:
    void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) override;

private:
    common::column_id_t columnToCopy;
    std::unique_ptr<NpyReader> reader;
};

// Note: This is a temporary class used specially in the node copier. NodeCopyTask mimics the
// behaviour of ProcessorTask. Eventually, we shouldn't have both CopyTask and NodeCopyTask.
class NodeCopyTask : public common::Task {
public:
    NodeCopyTask(
        std::unique_ptr<NodeCopier> nodeCopier, processor::ExecutionContext* executionContext)
        : Task{executionContext->numThreads}, nodeCopier{std::move(nodeCopier)},
          executionContext{executionContext} {};

    inline void run() override {
        mtx.lock();
        auto clonedNodeCopier = nodeCopier->clone();
        mtx.unlock();
        clonedNodeCopier->execute(executionContext);
    }
    inline void finalizeIfNecessary() override { nodeCopier->finalize(); }

private:
    std::mutex mtx;
    std::unique_ptr<NodeCopier> nodeCopier;
    processor::ExecutionContext* executionContext;
};

} // namespace storage
} // namespace kuzu
