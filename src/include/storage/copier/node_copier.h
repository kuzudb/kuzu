#pragma once

#include "storage/in_mem_storage_structure/node_in_mem_column.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "table_copier.h"

namespace kuzu {
namespace storage {

using set_element_func_t = std::function<void(NodeInMemColumn* column,
    InMemColumnChunk* columnChunk, common::offset_t nodeOffset, const std::string& data)>;

class NodeCopier : public TableCopier {

public:
    NodeCopier(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog, common::table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
        : TableCopier{copyDescription, std::move(outputDirectory), taskScheduler, catalog, tableID,
              nodesStatisticsAndDeletedIDs} {}

protected:
    void initializeColumnsAndLists() override;

    void populateColumnsAndLists() override;

    void saveToFile() override;

    template<typename T>
    static void populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
        common::NullMask* nullMask, HashIndexBuilder<T>* pkIndex, common::offset_t startOffset,
        uint64_t numValues);

    std::unordered_map<common::property_id_t, std::unique_ptr<NodeInMemColumn>> columns;

private:
    template<typename T>
    arrow::Status populateColumns();

    template<typename T>
    arrow::Status populateColumnsFromCSV(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromParquet(std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void putPropsOfLinesIntoColumns(InMemColumnChunk* columnChunk, NodeInMemColumn* column,
        std::shared_ptr<T> arrowArray, common::offset_t startNodeOffset,
        uint64_t numLinesInCurBlock, common::CopyDescription& copyDescription,
        PageByteCursor& overflowCursor);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    template<typename T1, typename T2>
    static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockIdx,
        uint64_t startOffset, HashIndexBuilder<T1>* pkIndex, NodeCopier* copier,
        const std::vector<std::shared_ptr<T2>>& batchColumns, std::string filePath);

    template<typename T>
    arrow::Status assignCopyCSVTasks(arrow::csv::StreamingReader* csvStreamingReader,
        common::offset_t startOffset, std::string filePath,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status assignCopyParquetTasks(parquet::arrow::FileReader* parquetReader,
        common::offset_t startOffset, std::string filePath,
        std::unique_ptr<HashIndexBuilder<T>>& pkIndex);

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
