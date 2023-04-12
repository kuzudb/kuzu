#pragma once

#include "storage/in_mem_storage_structure/node_in_mem_column.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "table_copier.h"

namespace kuzu {
namespace storage {

using lock_t = std::unique_lock<std::mutex>;

using set_element_func_t = std::function<void(NodeInMemColumn* column,
    InMemColumnChunk* columnChunk, common::offset_t nodeOffset, const std::string& data)>;

template<typename MORSEL_T>
class NodeCopyMorsel {

public:
    static constexpr common::block_idx_t INVALID_BLOCK_IDX = -1ull;

public:
    NodeCopyMorsel(common::offset_t startOffset, common::block_idx_t blockIdx)
        : startOffset{startOffset}, blockIdx{blockIdx} {};

    virtual ~NodeCopyMorsel() = default;

    virtual const std::vector<std::shared_ptr<MORSEL_T>>& getArrowColumns() = 0;

    bool success() { return blockIdx != INVALID_BLOCK_IDX; }

public:
    common::offset_t startOffset;
    common::block_idx_t blockIdx;
};

class CSVNodeCopyMorsel : public NodeCopyMorsel<arrow::Array> {

public:
    CSVNodeCopyMorsel(std::shared_ptr<arrow::RecordBatch> recordBatch, common::offset_t startOffset,
        common::block_idx_t blockIdx)
        : NodeCopyMorsel{startOffset, blockIdx}, recordBatch{recordBatch} {};

    const std::vector<std::shared_ptr<arrow::Array>>& getArrowColumns() override {
        return recordBatch->columns();
    }

private:
    std::shared_ptr<arrow::RecordBatch> recordBatch;
};

class ParquetNodeCopyMorsel : public NodeCopyMorsel<arrow::ChunkedArray> {

public:
    ParquetNodeCopyMorsel(std::shared_ptr<arrow::Table> currTable, common::offset_t startOffset,
        common::block_idx_t blockIdx)
        : NodeCopyMorsel{startOffset, blockIdx}, currTable{currTable} {};

    const std::vector<std::shared_ptr<arrow::ChunkedArray>>& getArrowColumns() override {
        return currTable->columns();
    }

private:
    std::shared_ptr<arrow::Table> currTable;
};

template<typename T1, typename T2>
class NodeCopySharedState {

public:
    NodeCopySharedState(
        std::string filePath, HashIndexBuilder<T1>* pkIndex, common::offset_t startOffset)
        : filePath{filePath}, pkIndex{pkIndex}, startOffset{startOffset}, blockIdx{0} {};

    virtual ~NodeCopySharedState() = default;

    virtual std::unique_ptr<NodeCopyMorsel<T2>> getMorsel() = 0;

public:
    std::string filePath;
    HashIndexBuilder<T1>* pkIndex;
    common::offset_t startOffset;

protected:
    common::block_idx_t blockIdx;
    std::mutex mtx;
};

template<typename T>
class CSVNodeCopySharedState : public NodeCopySharedState<T, arrow::Array> {

public:
    CSVNodeCopySharedState(std::string filePath, HashIndexBuilder<T>* pkIndex,
        common::offset_t startOffset,
        std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader)
        : NodeCopySharedState<T, arrow::Array>{filePath, pkIndex, startOffset},
          csvStreamingReader{move(csvStreamingReader)} {};
    std::unique_ptr<NodeCopyMorsel<arrow::Array>> getMorsel() override;

private:
    std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
};

template<typename T>
class ParquetNodeCopySharedState : public NodeCopySharedState<T, arrow::ChunkedArray> {

public:
    ParquetNodeCopySharedState(std::string filePath, HashIndexBuilder<T>* pkIndex,
        common::offset_t startOffset, uint64_t numBlocks,
        std::unique_ptr<parquet::arrow::FileReader> parquetReader)
        : NodeCopySharedState<T, arrow::ChunkedArray>{filePath, pkIndex, startOffset},
          numBlocks{numBlocks}, parquetReader{std::move(parquetReader)} {};
    std::unique_ptr<NodeCopyMorsel<arrow::ChunkedArray>> getMorsel() override;

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

    std::unordered_map<common::property_id_t, std::unique_ptr<NodeInMemColumn>> columns;

private:
    template<typename T>
    arrow::Status populateColumns(processor::ExecutionContext* executionContext);

    template<typename T>
    arrow::Status populateColumnsFromCSV(processor::ExecutionContext* executionContext,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromParquet(processor::ExecutionContext* executionContext,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void putPropsOfLinesIntoColumns(InMemColumnChunk* columnChunk, NodeInMemColumn* column,
        std::shared_ptr<T> arrowArray, common::offset_t startNodeOffset,
        uint64_t numLinesInCurBlock, common::CopyDescription& copyDescription,
        PageByteCursor& overflowCursor);

    // Concurrent tasks.
    template<typename T1, typename T2>
    static void batchPopulateColumnsTask(NodeCopySharedState<T1, T2>* sharedState,
        NodeCopier* copier, processor::ExecutionContext* executionContext);

    template<typename T>
    static void appendPKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::offset_t offset, HashIndexBuilder<T>* pkIndex) {
        assert(false);
    }

    static set_element_func_t getSetElementFunc(common::DataTypeID typeID,
        common::CopyDescription& copyDescription, PageByteCursor& pageByteCursor);

    template<typename T>
    inline static void setNumericElement(NodeInMemColumn* column, InMemColumnChunk* columnChunk,
        common::offset_t nodeOffset, const std::string& data) {
        auto val = common::TypeUtils::convertStringToNumber<T>(data.c_str());
        column->setElementInChunk(columnChunk, nodeOffset, reinterpret_cast<const uint8_t*>(&val));
    }

    inline static void setBoolElement(NodeInMemColumn* column, InMemColumnChunk* columnChunk,
        common::offset_t nodeOffset, const std::string& data) {
        auto val = common::TypeUtils::convertToBoolean(data.c_str());
        column->setElementInChunk(columnChunk, nodeOffset, reinterpret_cast<const uint8_t*>(&val));
    }

    template<typename T>
    inline static void setTimeElement(NodeInMemColumn* column, InMemColumnChunk* columnChunk,
        common::offset_t nodeOffset, const std::string& data) {
        auto val = T::FromCString(data.c_str(), data.length());
        column->setElementInChunk(columnChunk, nodeOffset, reinterpret_cast<const uint8_t*>(&val));
    }

    inline static void setStringElement(NodeInMemColumn* column, InMemColumnChunk* columnChunk,
        common::offset_t nodeOffset, const std::string& data, PageByteCursor& overflowCursor) {
        auto val = column->getInMemOverflowFile()->copyString(
            data.substr(0, common::BufferPoolConstants::PAGE_4KB_SIZE).c_str(), overflowCursor);
        column->setElementInChunk(columnChunk, nodeOffset, reinterpret_cast<uint8_t*>(&val));
    }

    inline static void setVarListElement(NodeInMemColumn* column, InMemColumnChunk* columnChunk,
        common::offset_t nodeOffset, const std::string& data,
        common::CopyDescription& copyDescription, PageByteCursor& overflowCursor) {
        auto varListVal =
            getArrowVarList(data, 1, data.length() - 2, column->getDataType(), copyDescription);
        auto kuList = column->getInMemOverflowFile()->copyList(*varListVal, overflowCursor);
        column->setElementInChunk(columnChunk, nodeOffset, reinterpret_cast<uint8_t*>(&kuList));
    }

    inline static void setFixedListElement(NodeInMemColumn* column, InMemColumnChunk* columnChunk,
        common::offset_t nodeOffset, const std::string& data,
        common::CopyDescription& copyDescription) {
        auto fixedListVal =
            getArrowFixedList(data, 1, data.length() - 2, column->getDataType(), copyDescription);
        column->setElementInChunk(columnChunk, nodeOffset, fixedListVal.get());
    }
};

} // namespace storage
} // namespace kuzu
