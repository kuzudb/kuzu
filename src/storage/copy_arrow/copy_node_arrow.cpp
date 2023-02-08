#include "storage/copy_arrow/copy_node_arrow.h"

#include "storage/copy_arrow/copy_task.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

CopyNodeArrow::CopyNodeArrow(CopyDescription& copyDescription, std::string outputDirectory,
    TaskScheduler& taskScheduler, Catalog& catalog, table_id_t tableID,
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs)
    : CopyStructuresArrow{copyDescription, std::move(outputDirectory), taskScheduler, catalog},
      nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs} {
    nodeTableSchema = catalog.getReadOnlyVersion()->getNodeTableSchema(tableID);
}

uint64_t CopyNodeArrow::copy() {
    auto read_start = std::chrono::high_resolution_clock::now();
    logger->info(
        "Reading " + CopyDescription::getFileTypeName(copyDescription.fileType) + " file.");

    countNumLines(copyDescription.filePath);
    initializeColumnsAndList();

    arrow::Status status;

    switch (nodeTableSchema->getPrimaryKey().dataType.typeID) {
    case INT64: {
        status = populateColumns<int64_t>();
    } break;
    case STRING: {
        status = populateColumns<ku_string_t>();
    } break;
    default: {
        throw CopyException("Unsupported data type " +
                            Types::dataTypeToString(nodeTableSchema->getPrimaryKey().dataType) +
                            " for the ID index.");
    }
    }

    throwCopyExceptionIfNotOK(status);

    auto read_end = std::chrono::high_resolution_clock::now();
    auto write_start = std::chrono::high_resolution_clock::now();

    saveToFile();
    nodesStatisticsAndDeletedIDs->setNumTuplesForTable(nodeTableSchema->tableID, numRows);
    logger->info("Done copying node {} with table {}.", nodeTableSchema->tableName,
        nodeTableSchema->tableID);

    auto write_end = std::chrono::high_resolution_clock::now();
    auto read_time = std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start);
    auto write_time =
        std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start);
    auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(write_end - read_start);
    logger->debug("read time: {}.", read_time.count());
    logger->debug("write time: {}.", write_time.count());
    logger->debug("total time: {}.", total_time.count());
    return numRows;
}

void CopyNodeArrow::initializeColumnsAndList() {
    logger->info("Initializing in memory columns.");
    columns.resize(nodeTableSchema->getNumProperties());
    for (auto& property : nodeTableSchema->properties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(outputDirectory,
            nodeTableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
        columns[property.propertyID] =
            InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numRows);
    }
    logger->info("Done initializing in memory columns.");
}

template<typename T>
arrow::Status CopyNodeArrow::populateColumns() {
    logger->info("Populating properties");
    auto pkIndex =
        make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                             nodeTableSchema->tableID, DBFileType::WAL_VERSION),
            nodeTableSchema->getPrimaryKey().dataType);
    pkIndex->bulkReserve(numRows);

    arrow::Status status;
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV:
        status = populateColumnsFromCSV<T>(pkIndex);
        break;
    case CopyDescription::FileType::ARROW:
        status = populateColumnsFromArrow<T>(pkIndex);
        break;
    case CopyDescription::FileType::PARQUET:
        status = populateColumnsFromParquet<T>(pkIndex);
        break;
    }

    logger->info("Flush the pk index to disk.");
    pkIndex->flush();
    logger->info("Done populating properties, constructing the pk index.");
    return status;
}

template<typename T>
arrow::Status CopyNodeArrow::populateColumnsFromCSV(std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    offset_t offsetStart = 0;

    std::shared_ptr<arrow::csv::StreamingReader> csv_streaming_reader;
    auto status = initCSVReaderAndCheckStatus(csv_streaming_reader, copyDescription.filePath);

    std::shared_ptr<arrow::RecordBatch> currBatch;
    int blockIdx = 0;
    auto it = csv_streaming_reader->begin();
    auto endIt = csv_streaming_reader->end();
    while (it != endIt) {
        for (int i = 0; i < CopyConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (it == endIt) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                batchPopulateColumnsTask<T, arrow::Array>, nodeTableSchema->primaryKeyPropertyID,
                blockIdx, offsetStart, pkIndex.get(), this, currBatch->columns(), copyDescription));
            offsetStart += currBatch->num_rows();
            ++blockIdx;
            ++it;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
arrow::Status CopyNodeArrow::populateColumnsFromArrow(
    std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    offset_t offsetStart = 0;

    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
    auto status = initArrowReaderAndCheckStatus(ipc_reader, copyDescription.filePath);

    std::shared_ptr<arrow::RecordBatch> currBatch;

    int blockIdx = 0;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < CopyConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, ipc_reader->ReadRecordBatch(blockIdx));
            taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
                batchPopulateColumnsTask<T, arrow::Array>, nodeTableSchema->primaryKeyPropertyID,
                blockIdx, offsetStart, pkIndex.get(), this, currBatch->columns(), copyDescription));
            offsetStart += currBatch->num_rows();
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }

    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
arrow::Status CopyNodeArrow::populateColumnsFromParquet(
    std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    offset_t offsetStart = 0;

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = initParquetReaderAndCheckStatus(reader, copyDescription.filePath);

    std::shared_ptr<arrow::Table> currTable;
    int blockIdx = 0;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < CopyConfig::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_RETURN_NOT_OK(reader->RowGroup(blockIdx)->ReadTable(&currTable));
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(batchPopulateColumnsTask<T, arrow::ChunkedArray>,
                    nodeTableSchema->primaryKeyPropertyID, blockIdx, offsetStart, pkIndex.get(),
                    this, currTable->columns(), copyDescription));
            offsetStart += currTable->num_rows();
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConfig::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }

    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
void CopyNodeArrow::populatePKIndex(
    InMemColumn* column, HashIndexBuilder<T>* pkIndex, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if constexpr (std::is_same<T, int64_t>::value) {
            auto key = (int64_t*)column->getElement(offset);
            if (!pkIndex->append(*key, offset)) {
                throw CopyException(Exception::getExistedPKExceptionMsg(std::to_string(*key)));
            }
        } else {
            auto element = (ku_string_t*)column->getElement(offset);
            auto key = column->getInMemOverflowFile()->readString(element);
            if (!pkIndex->append(key.c_str(), offset)) {
                throw CopyException(Exception::getExistedPKExceptionMsg(key));
            }
        }
    }
}

template<typename T1, typename T2>
arrow::Status CopyNodeArrow::batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx,
    uint64_t blockId, uint64_t offsetStart, HashIndexBuilder<T1>* pkIndex, CopyNodeArrow* copier,
    const std::vector<std::shared_ptr<T2>>& batchColumns, CopyDescription& copyDescription) {
    copier->logger->trace("Start: path={0} blkIdx={1}", copier->copyDescription.filePath, blockId);
    std::vector<PageByteCursor> overflowCursors(copier->nodeTableSchema->getNumProperties());
    for (auto blockOffset = 0u; blockOffset < copier->numLinesPerBlock[blockId]; ++blockOffset) {
        putPropsOfLineIntoColumns(copier->columns, overflowCursors, batchColumns,
            offsetStart + blockOffset, blockOffset, copyDescription);
    }
    populatePKIndex(copier->columns[primaryKeyPropertyIdx].get(), pkIndex, offsetStart,
        copier->numLinesPerBlock[blockId]);
    copier->logger->trace("End: path={0} blkIdx={1}", copier->copyDescription.filePath, blockId);
    return arrow::Status::OK();
}

template<typename T>
void CopyNodeArrow::putPropsOfLineIntoColumns(
    std::vector<std::unique_ptr<InMemColumn>>& structuredColumns,
    std::vector<PageByteCursor>& overflowCursors,
    const std::vector<std::shared_ptr<T>>& arrow_columns, uint64_t nodeOffset, uint64_t blockOffset,
    CopyDescription& copyDescription) {
    for (auto columnIdx = 0u; columnIdx < structuredColumns.size(); columnIdx++) {
        auto column = structuredColumns[columnIdx].get();
        auto currentToken = arrow_columns[columnIdx]->GetScalar(blockOffset);
        if ((*currentToken)->is_valid) {
            auto stringToken = currentToken->get()->ToString().substr(0, DEFAULT_PAGE_SIZE);
            const char* data = stringToken.c_str();

            switch (column->getDataType().typeID) {
            case INT64: {
                int64_t val = TypeUtils::convertToInt64(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case DOUBLE: {
                double_t val = TypeUtils::convertToDouble(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case BOOL: {
                bool val = TypeUtils::convertToBoolean(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case DATE: {
                date_t val = Date::FromCString(data, stringToken.length());
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case TIMESTAMP: {
                timestamp_t val = Timestamp::FromCString(data, stringToken.length());
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case INTERVAL: {
                interval_t val = Interval::FromCString(data, stringToken.length());
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case STRING: {
                auto val =
                    column->getInMemOverflowFile()->copyString(data, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case LIST: {
                auto listVal = getArrowList(stringToken, 1, stringToken.length() - 2,
                    column->getDataType(), copyDescription);
                auto kuList =
                    column->getInMemOverflowFile()->copyList(*listVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&kuList));
            } break;
            default:
                break;
            }
        }
    }
}

void CopyNodeArrow::saveToFile() {
    logger->debug("Writing node columns to disk.");
    assert(!columns.empty());
    for (auto& column : columns) {
        taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
            [&](InMemColumn* x) { x->saveToFile(); }, column.get()));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing node columns to disk.");
}

} // namespace storage
} // namespace kuzu
