#pragma once

#include "in_mem_structures_copier.h"
#include "storage/index/hash_index_builder.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace kuzu {
namespace storage {

class InMemArrowNodeCopier : public InMemStructuresCopier {

public:
    InMemArrowNodeCopier(CSVDescription& csvDescription, string outputDirectory,
        TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs);

    ~InMemArrowNodeCopier() override = default;

    uint64_t copy();

    void saveToFile() override;

    enum class FileTypes { CSV, ARROW, PARQUET };

    static std::string getFileTypeName(FileTypes fileTypes);

    static std::string getFileTypeSuffix(FileTypes fileTypes);

private:
    void setFileType(std::string const& fileName);

    void countNumLines(const std::string& filePath);

    arrow::Status countNumLinesCSV(std::string const& filePath);

    arrow::Status countNumLinesArrow(std::string const& filePath);

    arrow::Status countNumLinesParquet(std::string const& filePath);

    void initializeColumnsAndList();

    template<typename T>
    arrow::Status populateColumns();

    template<typename T>
    arrow::Status populateColumnsFromCSV(unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromArrow(unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    arrow::Status populateColumnsFromParquet(unique_ptr<HashIndexBuilder<T>>& pkIndex);

    template<typename T>
    static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>>& columns,
        vector<PageByteCursor>& overflowCursors, const std::vector<shared_ptr<T>>& arrow_columns,
        uint64_t nodeOffset, uint64_t bufferOffset, char delimiter);

    template<typename T>
    static void populatePKIndex(InMemColumn* column, HashIndexBuilder<T>* pkIndex,
        node_offset_t startOffset, uint64_t numValues);

    // Concurrent tasks.
    // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
    // Instead, it is the index in the structured columns that we expect it to appear.

    template<typename T1, typename T2>
    static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx, uint64_t blockId,
        uint64_t offsetStart, HashIndexBuilder<T1>* pkIndex, InMemArrowNodeCopier* copier,
        const vector<shared_ptr<T2>>& batchColumns, char delimiter);

    arrow::Status initCSVReader(shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
        const std::string& filePath);

    arrow::Status initArrowReader(std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader,
        const std::string& filePath);

    arrow::Status initParquetReader(std::unique_ptr<parquet::arrow::FileReader>& reader,
        const std::string& filePath);

    static Literal getArrowList(string& l, int64_t from, int64_t to, const DataType& dataType,
        char delimiter);

private:
    NodeTableSchema* nodeTableSchema;
    uint64_t numNodes;
    vector<unique_ptr<InMemColumn>> structuredColumns;
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    FileTypes inputFileType;
};

} // namespace storage
} // namespace kuzu
