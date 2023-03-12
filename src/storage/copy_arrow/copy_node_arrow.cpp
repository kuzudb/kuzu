#include "storage/copy_arrow/copy_node_arrow.h"

#include "storage/copy_arrow/copy_task.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void CopyNodeArrow::initializeColumnsAndLists() {
    logger->info("Initializing in memory columns.");
    columns.resize(tableSchema->getNumProperties());
    for (auto& property : tableSchema->properties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(
            outputDirectory, tableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
        columns[property.propertyID] =
            InMemColumnFactory::getInMemPropertyColumn(fName, property.dataType, numRows);
    }
    logger->info("Done initializing in memory columns.");
}

void CopyNodeArrow::populateColumnsAndLists() {
    arrow::Status status;
    auto primaryKey = reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey();
    switch (primaryKey.dataType.typeID) {
    case INT64: {
        status = populateColumns<int64_t>();
    } break;
    case STRING: {
        status = populateColumns<ku_string_t>();
    } break;
    default: {
        throw CopyException(StringUtils::string_format("Unsupported data type {} for the ID index.",
            Types::dataTypeToString(primaryKey.dataType)));
    }
    }
    throwCopyExceptionIfNotOK(status);
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

template<typename T>
arrow::Status CopyNodeArrow::populateColumns() {
    logger->info("Populating properties");
    auto pkIndex =
        std::make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                                  tableSchema->tableID, DBFileType::WAL_VERSION),
            reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey().dataType);
    pkIndex->bulkReserve(numRows);
    arrow::Status status;
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV:
        status = populateColumnsFromFiles<T>(pkIndex);
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
arrow::Status CopyNodeArrow::populateColumnsFromFiles(
    std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    for (auto& filePath : copyDescription.filePaths) {
        offset_t startOffset = fileBlockInfos.at(filePath).startOffset;
        std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
        auto status = initCSVReaderAndCheckStatus(csvStreamingReader, filePath);
        status = assignCopyTasks<T>(csvStreamingReader, startOffset, filePath, pkIndex);
        throwCopyExceptionIfNotOK(status);
    }
    return arrow::Status::OK();
}

template<typename T>
arrow::Status CopyNodeArrow::populateColumnsFromArrow(
    std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader;
    auto status = initArrowReaderAndCheckStatus(ipc_reader, copyDescription.filePaths[0]);
    std::shared_ptr<arrow::RecordBatch> currBatch;
    int blockIdx = 0;
    offset_t startOffset = 0;
    auto numBlocksInFile = fileBlockInfos.at(copyDescription.filePaths[0]).numBlocks;
    while (blockIdx < numBlocksInFile) {
        for (int i = 0; i < CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocksInFile) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, ipc_reader->ReadRecordBatch(blockIdx));
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(batchPopulateColumnsTask<T, arrow::Array>,
                    reinterpret_cast<NodeTableSchema*>(tableSchema)->primaryKeyPropertyID, blockIdx,
                    startOffset, pkIndex.get(), this, currBatch->columns(),
                    copyDescription.filePaths[0]));
            startOffset += currBatch->num_rows();
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
arrow::Status CopyNodeArrow::populateColumnsFromParquet(
    std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = initParquetReaderAndCheckStatus(reader, copyDescription.filePaths[0]);
    std::shared_ptr<arrow::Table> currTable;
    int blockIdx = 0;
    offset_t startOffset = 0;
    auto numBlocks = fileBlockInfos.at(copyDescription.filePaths[0]).numBlocks;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_RETURN_NOT_OK(reader->RowGroup(blockIdx)->ReadTable(&currTable));
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(batchPopulateColumnsTask<T, arrow::ChunkedArray>,
                    reinterpret_cast<NodeTableSchema*>(tableSchema)->primaryKeyPropertyID, blockIdx,
                    startOffset, pkIndex.get(), this, currTable->columns(),
                    copyDescription.filePaths[0]));
            startOffset += currTable->num_rows();
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<typename T>
void CopyNodeArrow::populatePKIndex(
    InMemColumn* column, HashIndexBuilder<T>* pkIndex, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if (column->isNullAtNodeOffset(offset)) {
            throw ReaderException("Primary key cannot be null.");
        }
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
    uint64_t blockIdx, uint64_t startOffset, HashIndexBuilder<T1>* pkIndex, CopyNodeArrow* copier,
    const std::vector<std::shared_ptr<T2>>& batchColumns, std::string filePath) {
    copier->logger->trace("Start: path={0} blkIdx={1}", filePath, blockIdx);
    std::vector<PageByteCursor> overflowCursors(copier->tableSchema->getNumProperties());
    auto numLinesInCurBlock = copier->fileBlockInfos.at(filePath).numLinesPerBlock[blockIdx];
    for (auto blockOffset = 0u; blockOffset < numLinesInCurBlock; ++blockOffset) {
        putPropsOfLineIntoColumns(copier->columns, overflowCursors, batchColumns,
            startOffset + blockOffset, blockOffset, copier->copyDescription);
    }
    populatePKIndex(
        copier->columns[primaryKeyPropertyIdx].get(), pkIndex, startOffset, numLinesInCurBlock);
    copier->logger->trace(
        "End: path={0} blkIdx={1}", copier->copyDescription.filePaths[0], blockIdx);
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
            auto stringToken = currentToken->get()->ToString();
            const char* data = stringToken.c_str();
            switch (column->getDataType().typeID) {
            case INT64: {
                auto val = TypeUtils::convertStringToNumber<int64_t>(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case INT32: {
                auto val = TypeUtils::convertStringToNumber<int32_t>(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case INT16: {
                auto val = TypeUtils::convertStringToNumber<int16_t>(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case DOUBLE: {
                auto val = TypeUtils::convertStringToNumber<double_t>(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case FLOAT: {
                auto val = TypeUtils::convertStringToNumber<float_t>(data);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case BOOL: {
                auto val = TypeUtils::convertToBoolean(data);
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
                stringToken = stringToken.substr(0, BufferPoolConstants::DEFAULT_PAGE_SIZE);
                data = stringToken.c_str();
                auto val =
                    column->getInMemOverflowFile()->copyString(data, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case VAR_LIST: {
                auto varListVal = getArrowVarList(stringToken, 1, stringToken.length() - 2,
                    column->getDataType(), copyDescription);
                auto kuList = column->getInMemOverflowFile()->copyList(
                    *varListVal, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&kuList));
            } break;
            case FIXED_LIST: {
                auto fixedListVal = getArrowFixedList(stringToken, 1, stringToken.length() - 2,
                    column->getDataType(), copyDescription);
                column->setElement(nodeOffset, fixedListVal.get());
            } break;
            default:
                break;
            }
        }
    }
}

template<typename T>
arrow::Status CopyNodeArrow::assignCopyTasks(
    std::shared_ptr<arrow::csv::StreamingReader>& csv_streaming_reader, offset_t startOffset,
    std::string filePath, std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    auto it = csv_streaming_reader->begin();
    auto endIt = csv_streaming_reader->end();
    std::shared_ptr<arrow::RecordBatch> currBatch;
    int blockIdx = 0;
    while (it != endIt) {
        for (int i = 0; i < common::CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (it == endIt) {
                break;
            }
            ARROW_ASSIGN_OR_RAISE(currBatch, *it);
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(batchPopulateColumnsTask<T, arrow::Array>,
                    reinterpret_cast<NodeTableSchema*>(tableSchema)->primaryKeyPropertyID, blockIdx,
                    startOffset, pkIndex.get(), this, currBatch->columns(), filePath));
            startOffset += currBatch->num_rows();
            ++blockIdx;
            ++it;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

} // namespace storage
} // namespace kuzu
