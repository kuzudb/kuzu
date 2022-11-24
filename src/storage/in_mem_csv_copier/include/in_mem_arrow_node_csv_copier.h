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

    private:
        uint64_t copyFromCSVFile();

        uint64_t copyFromArrowFile();

        static uint64_t getFileType(std::string const &fileName);

        arrow::Status initializeArrowCSV(const string &filePath);

        arrow::Status initializeArrowArrow(std::shared_ptr<arrow::io::ReadableFile> &infile,
                                           shared_ptr<arrow::ipc::RecordBatchFileReader> &ipc_reader);

        void initializeColumnsAndList();

        template<typename T>
        arrow::Status csvPopulateColumns();

        template<typename T>
        arrow::Status arrowPopulateColumns(shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader);

        static void putPropsOfLineIntoColumns(vector<unique_ptr<InMemColumn>> &columns,
                                              const vector<Property> &properties,
                                              vector<PageByteCursor> &overflowCursors,
                                              std::vector<shared_ptr<arrow::Array>> &arrow_columns,
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

        template<typename T>
        static arrow::Status batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx,
                                                      uint64_t blockId, uint64_t offsetStart,
                                                      HashIndexBuilder<T> *pkIndex,
                                                      InMemArrowNodeCSVCopier *copier,
                                                      vector<shared_ptr<arrow::Array>> &batchColumns);

    private:
        NodeTableSchema *nodeTableSchema;
        uint64_t numNodes;
        vector<unique_ptr<InMemColumn>> structuredColumns;
        NodesStatisticsAndDeletedIDs *nodesStatisticsAndDeletedIDs;
    };

    const std::string InMemArrowNodeCSVCopier::CSV_SUFFIX = ".csv";
    const std::string InMemArrowNodeCSVCopier::ARROW_SUFFIX = ".arrow";

} // namespace storage
} // namespace kuzu
