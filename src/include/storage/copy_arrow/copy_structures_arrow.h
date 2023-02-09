#pragma once

#include "catalog/catalog.h"
#include "common/csv_reader/csv_reader.h"
#include "common/logging_level_utils.h"
#include "common/task_system/task_scheduler.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"
#include "storage/in_mem_storage_structure/in_mem_lists.h"
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

class CopyStructuresArrow {
protected:
    CopyStructuresArrow(common::CopyDescription& copyDescription, std::string outputDirectory,
        common::TaskScheduler& taskScheduler, catalog::Catalog& catalog);

    virtual ~CopyStructuresArrow() = default;

public:
    virtual void saveToFile() = 0;

protected:
    // Initializes (in listHeadersBuilder) the header of each list in a Lists structure, from the
    // listSizes. ListSizes is used to determine if the list is small or large, based on which,
    // information is encoded in the 4 byte header.
    static void calculateListHeadersTask(common::offset_t numNodes, uint32_t elementSize,
        atomic_uint64_vec_t* listSizes, ListHeadersBuilder* listHeadersBuilder,
        const std::shared_ptr<spdlog::logger>& logger);

    // Initializes Metadata information of a Lists structure, that is chunksPagesMap and
    // largeListsPagesMap, using listSizes and listHeadersBuilder.
    // **Note that this file also allocates the in-memory pages of the InMemFile that will actually
    // store the data in the lists (e.g., neighbor ids or edge properties).
    static void calculateListsMetadataAndAllocateInMemListPagesTask(uint64_t numNodes,
        uint32_t elementSize, atomic_uint64_vec_t* listSizes,
        ListHeadersBuilder* listHeadersBuilder, InMemLists* inMemList, bool hasNULLBytes,
        const std::shared_ptr<spdlog::logger>& logger);

    void countNumLines(const std::string& filePath);

    arrow::Status countNumLinesCSV(std::string const& filePath);

    arrow::Status countNumLinesArrow(std::string const& filePath);

    arrow::Status countNumLinesParquet(std::string const& filePath);

    arrow::Status initCSVReaderAndCheckStatus(
        std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
        const std::string& filePath);
    arrow::Status initCSVReader(std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader,
        const std::string& filePath);

    arrow::Status initArrowReaderAndCheckStatus(
        std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader,
        const std::string& filePath);
    arrow::Status initArrowReader(std::shared_ptr<arrow::ipc::RecordBatchFileReader>& ipc_reader,
        const std::string& filePath);

    arrow::Status initParquetReaderAndCheckStatus(
        std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath);
    arrow::Status initParquetReader(
        std::unique_ptr<parquet::arrow::FileReader>& reader, const std::string& filePath);

    static std::unique_ptr<common::Value> getArrowList(std::string& l, int64_t from, int64_t to,
        const common::DataType& dataType, common::CopyDescription& CopyDescription);

    static void throwCopyExceptionIfNotOK(const arrow::Status& status);

protected:
    std::shared_ptr<spdlog::logger> logger;
    common::CopyDescription& copyDescription;
    std::string outputDirectory;
    uint64_t numBlocks;
    std::vector<uint64_t> numLinesPerBlock;
    common::TaskScheduler& taskScheduler;
    catalog::Catalog& catalog;
    uint64_t numRows;
};

} // namespace storage
} // namespace kuzu
