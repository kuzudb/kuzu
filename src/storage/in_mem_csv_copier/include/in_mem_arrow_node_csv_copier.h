#pragma once

#include "in_mem_structures_csv_copier.h"

#include "src/storage/index/include/hash_index_builder.h"
#include "src/storage/store/include/nodes_statistics_and_deleted_ids.h"

#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/ipc/reader.h>
#include <arrow/scalar.h>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace kuzu {
namespace storage {

    class InMemArrowNodeCSVCopier : public InMemStructuresCSVCopier {

    public:
        InMemArrowNodeCSVCopier(CSVDescription &csvDescription, string outputDirectory,
                                TaskScheduler &taskScheduler, Catalog &catalog, table_id_t tableID,
                                NodesStatisticsAndDeletedIDs *nodesStatisticsAndDeletedIDs);

        ~InMemArrowNodeCSVCopier() override = default;

        uint64_t copy();

        void saveToFile() override;

        static const std::string CSV_SUFFIX;
        static const std::string ARROW_SUFFIX;
        static const std::string PARQUET_SUFFIX;

    private:
        uint64_t copyFromCSVFile();

        uint64_t copyFromArrowFile();

        uint64_t copyFromParquetFile();

        static uint64_t getFileType(std::string const &fileName);

        arrow::Status initializeArrowCSV(const string &filePath);

        arrow::Status initializeArrowArrow(std::shared_ptr<arrow::io::ReadableFile> &infile,
                                           std::shared_ptr<arrow::ipc::RecordBatchFileReader> &ipc_reader);

        void initializeArrowParquet(std::shared_ptr<arrow::io::ReadableFile> &infile,
                                    std::unique_ptr<parquet::arrow::FileReader> &reader);

        void initializeColumnsAndList();

        template<typename T>
        arrow::Status csvPopulateColumns();

        template<typename T>
        arrow::Status arrowPopulateColumns(const shared_ptr<arrow::ipc::RecordBatchFileReader> &ipc_reader);

        template<typename T>
        void parquetPopulateColumns(const std::unique_ptr<parquet::arrow::FileReader> &reader);

        template<typename T>
        static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>> &columns,
                                              const vector<Property> &properties,
                                              vector<PageByteCursor> &overflowCursors,
                                              const std::vector<shared_ptr<T>> &arrow_columns,
                                              uint64_t nodeOffset, uint64_t bufferOffset);

        template<typename T>
        static void addIDsToIndex(InMemColumn *column, HashIndexBuilder<T> *hashIndex,
                                  node_offset_t startOffset, uint64_t numValues);

        template<typename T>
        static void populatePKIndex(InMemColumn *column, HashIndexBuilder<T> *pkIndex,
                                    node_offset_t startOffset, uint64_t numValues);

        // Concurrent tasks.
        // Note that primaryKeyPropertyIdx is *NOT* the property ID of the primary key property.
        // Instead, it is the index in the structured columns that we expect it to appear.

        template<typename T1, typename T2>
        static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx,
                                                      uint64_t blockId, uint64_t offsetStart,
                                                      HashIndexBuilder<T1> *pkIndex,
                                                      InMemArrowNodeCSVCopier *copier,
                                                      const vector<shared_ptr<T2>> &batchColumns);

    private:
        NodeTableSchema *nodeTableSchema;
        uint64_t numNodes;
        vector<unique_ptr<InMemColumn>> structuredColumns;
        NodesStatisticsAndDeletedIDs *nodesStatisticsAndDeletedIDs;
    };

    const std::string InMemArrowNodeCSVCopier::CSV_SUFFIX = ".csv";
    const std::string InMemArrowNodeCSVCopier::ARROW_SUFFIX = ".arrow";
    const std::string InMemArrowNodeCSVCopier::PARQUET_SUFFIX = ".parquet";

} // namespace storage
} // namespace kuzu
