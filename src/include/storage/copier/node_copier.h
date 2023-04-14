#pragma once

#include "storage/in_mem_storage_structure/in_mem_node_column.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "table_copier.h"

namespace kuzu {
namespace storage {

class NodeCopyMorsel {
public:
    static constexpr common::block_idx_t INVALID_BLOCK_IDX = UINT64_MAX;

public:
    NodeCopyMorsel(common::offset_t startOffset, common::block_idx_t blockIdx,
        std::shared_ptr<arrow::RecordBatch> recordBatch)
        : startOffset{startOffset}, blockIdx{blockIdx}, recordBatch{std::move(recordBatch)} {};

    virtual ~NodeCopyMorsel() = default;

    bool success() const { return blockIdx != INVALID_BLOCK_IDX; }

public:
    common::offset_t startOffset;
    common::block_idx_t blockIdx;
    std::shared_ptr<arrow::RecordBatch> recordBatch;
};

template<typename T>
class NodeCopySharedState {

public:
    NodeCopySharedState(
        std::string filePath, HashIndexBuilder<T>* pkIndex, common::offset_t startOffset)
        : filePath{std::move(filePath)}, pkIndex{pkIndex}, startOffset{startOffset}, blockIdx{0} {};

    virtual ~NodeCopySharedState() = default;

    virtual std::unique_ptr<NodeCopyMorsel> getMorsel() = 0;

public:
    std::string filePath;
    HashIndexBuilder<T>* pkIndex;
    common::offset_t startOffset;

protected:
    common::block_idx_t blockIdx;
    std::mutex mtx;
};

template<typename T>
class CSVNodeCopySharedState : public NodeCopySharedState<T> {

public:
    CSVNodeCopySharedState(std::string filePath, HashIndexBuilder<T>* pkIndex,
        common::offset_t startOffset,
        std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader)
        : NodeCopySharedState<T>{filePath, pkIndex, startOffset}, csvStreamingReader{std::move(
                                                                      csvStreamingReader)} {};
    std::unique_ptr<NodeCopyMorsel> getMorsel() override;

private:
    std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
};

template<typename T>
class ParquetNodeCopySharedState : public NodeCopySharedState<T> {

public:
    ParquetNodeCopySharedState(std::string filePath, HashIndexBuilder<T>* pkIndex,
        common::offset_t startOffset, uint64_t numBlocks,
        std::unique_ptr<parquet::arrow::FileReader> parquetReader)
        : NodeCopySharedState<T>{filePath, pkIndex, startOffset}, numBlocks{numBlocks},
          parquetReader{std::move(parquetReader)} {};
    std::unique_ptr<NodeCopyMorsel> getMorsel() override;

public:
    uint64_t numBlocks;

private:
    std::unique_ptr<parquet::arrow::FileReader> parquetReader;
};

class NodeCopier : public TableCopier {

public:
    NodeCopier(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
        : TableCopier{copyDescription, std::move(outputDirectory), taskScheduler, catalog, tableID,
              nodesStatisticsAndDeletedIDs} {}

protected:
    void initializeColumnsAndLists() override;

    void populateColumnsAndLists(processor::ExecutionContext* executionContext) override;

    void saveToFile() override;

    template<typename T>
    static void populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::NullMask* nullMask, HashIndexBuilder<T>* pkIndex, common::offset_t startOffset,
        uint64_t numValues);

    std::unordered_map<common::property_id_t, std::unique_ptr<InMemNodeColumn>> columns;

private:
    template<typename T>
    void populateColumns(processor::ExecutionContext* executionContext);

    template<typename T>
    void populateColumnsFromCSV(processor::ExecutionContext* executionContext,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    void populateColumnsFromParquet(processor::ExecutionContext* executionContext,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    static void copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk, InMemNodeColumn* column,
        arrow::Array& arrowArray, common::offset_t startNodeOffset,
        common::CopyDescription& copyDescription, PageByteCursor& overflowCursor);

    // Concurrent tasks.
    template<typename T>
    static void populateColumnChunksTask(NodeCopySharedState<T>* sharedState, NodeCopier* copier,
        processor::ExecutionContext* executionContext, spdlog::logger& logger);

    template<typename T>
    static void appendPKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::offset_t offset, HashIndexBuilder<T>* pkIndex) {
        assert(false);
    }
};

} // namespace storage
} // namespace kuzu
