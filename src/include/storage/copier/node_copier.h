#pragma once

#include "storage/copier/npy_reader.h"
#include "storage/copier/table_copy_executor.h"
#include "storage/in_mem_storage_structure/in_mem_node_column.h"
#include "storage/index/hash_index_builder.h"
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
    // todo: do we need this?
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

template<typename T>
class NodeCopier {
public:
    NodeCopier(std::shared_ptr<NodeCopySharedState> sharedState, HashIndexBuilder<T>* pkIndex,
        const common::CopyDescription& copyDesc, std::vector<InMemNodeColumn*> columns,
        common::column_id_t pkColumnID)
        : sharedState{std::move(sharedState)}, pkIndex{pkIndex}, copyDesc{copyDesc},
          columns{columns}, pkColumnID{pkColumnID} {
        overflowCursors.resize(columns.size());
    }
    virtual ~NodeCopier() = default;

    void execute(processor::ExecutionContext* executionContext);

    inline virtual void finalize() { pkIndex->flush(); }

    virtual std::unique_ptr<NodeCopier> clone() const {
        return std::make_unique<NodeCopier>(sharedState, pkIndex, copyDesc, columns, pkColumnID);
    }

protected:
    virtual void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
        throw std::runtime_error("Not implemented");
    }

    static void copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk, InMemNodeColumn* column,
        arrow::Array& arrowArray, common::offset_t startNodeOffset,
        common::CopyDescription& copyDescription, PageByteCursor& overflowCursor);
    static void populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::NullMask* nullMask, HashIndexBuilder<T>* pkIndex, common::offset_t startOffset,
        uint64_t numValues);
    static void appendPKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::offset_t offset, HashIndexBuilder<T>* pkIndex) {
        assert(false);
    }

protected:
    std::shared_ptr<NodeCopySharedState> sharedState;
    HashIndexBuilder<T>* pkIndex;
    common::CopyDescription copyDesc;
    std::vector<InMemNodeColumn*> columns;
    common::column_id_t pkColumnID;
    std::vector<PageByteCursor> overflowCursors;
};

template<>
void NodeCopier<int64_t>::appendPKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    common::offset_t offset, HashIndexBuilder<int64_t>* pkIndex);
template<>
void NodeCopier<common::ku_string_t>::appendPKIndex(InMemColumnChunk* chunk,
    InMemOverflowFile* overflowFile, common::offset_t offset,
    HashIndexBuilder<common::ku_string_t>* pkIndex);

template<typename T>
class CSVNodeCopier : public NodeCopier<T> {
public:
    CSVNodeCopier(std::shared_ptr<NodeCopySharedState> sharedState, HashIndexBuilder<T>* pkIndex,
        const common::CopyDescription& copyDesc, std::vector<InMemNodeColumn*> columns,
        common::column_id_t pkColumnID)
        : NodeCopier<T>{std::move(sharedState), pkIndex, copyDesc, columns, pkColumnID} {}

    inline std::unique_ptr<NodeCopier<T>> clone() const override {
        return std::make_unique<CSVNodeCopier<T>>(
            this->sharedState, this->pkIndex, this->copyDesc, this->columns, this->pkColumnID);
    }

protected:
    void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) override;
};

template<typename T>
class ParquetNodeCopier : public NodeCopier<T> {
public:
    ParquetNodeCopier(std::shared_ptr<NodeCopySharedState> sharedState,
        HashIndexBuilder<T>* pkIndex, const common::CopyDescription& copyDesc,
        std::vector<InMemNodeColumn*> columns, common::column_id_t pkColumnID)
        : NodeCopier<T>{std::move(sharedState), pkIndex, copyDesc, columns, pkColumnID} {}

    inline std::unique_ptr<NodeCopier<T>> clone() const override {
        return std::make_unique<ParquetNodeCopier<T>>(
            this->sharedState, this->pkIndex, this->copyDesc, this->columns, this->pkColumnID);
    }

protected:
    void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) override;

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

template<typename T>
class NPYNodeCopier : public NodeCopier<T> {
public:
    NPYNodeCopier(std::shared_ptr<NodeCopySharedState> sharedState, HashIndexBuilder<T>* pkIndex,
        const common::CopyDescription& copyDesc, std::vector<InMemNodeColumn*> columns,
        common::column_id_t columnID, common::column_id_t pkColumnID)
        : NodeCopier<T>{std::move(sharedState), pkIndex, copyDesc, columns, pkColumnID},
          columnID{columnID} {}

    inline std::unique_ptr<NodeCopier<T>> clone() const override {
        return std::make_unique<NPYNodeCopier<T>>(this->sharedState, this->pkIndex, this->copyDesc,
            this->columns, columnID, this->pkColumnID);
    }

    inline void finalize() override {
        if (this->pkColumnID != common::INVALID_COLUMN_ID) {
            this->pkIndex->flush();
        }
    }

protected:
    void executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) override;

private:
    std::unique_ptr<NpyReader> reader;
    common::column_id_t columnID;
};

// Note: This is a temporary class used specially in the node copier. NodeCopyTask mimics the
// behaviour of ProcessorTask. Eventually, we shouldn't have both CopyTask and NodeCopyTask.
template<typename T>
class NodeCopyTask : public common::Task {
public:
    NodeCopyTask(
        std::unique_ptr<NodeCopier<T>> nodeCopier, processor::ExecutionContext* executionContext)
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
    std::unique_ptr<NodeCopier<T>> nodeCopier;
    processor::ExecutionContext* executionContext;
};

} // namespace storage
} // namespace kuzu
