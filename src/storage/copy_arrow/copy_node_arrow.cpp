#include "storage/copy_arrow/copy_node_arrow.h"

#include "storage/copy_arrow/copy_task.h"

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
        columns[property.propertyID] = InMemBMPageCollectionFactory::getInMemBMPageCollection(
            fName, property.dataType, numRows, bufferManager);
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
            [&](BMBackedInMemColumn* x) { x->saveToFile(); }, column.get()));
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
        status = populateColumnsFromCSV<T>(pkIndex);
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
    for (auto& filePath : copyDescription.filePaths) {
        std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader;
        auto status = initCSVReaderAndCheckStatus(csvStreamingReader, filePath);
        status = assignCopyCSVTasks<T>(
            csvStreamingReader.get(), fileBlockInfos.at(filePath).startOffset, filePath, pkIndex);
        throwCopyExceptionIfNotOK(status);
    }
    return arrow::Status::OK();
}

template<typename T>
arrow::Status CopyNodeArrow::populateColumnsFromParquet(
    std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    for (auto& filePath : copyDescription.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        auto status = initParquetReaderAndCheckStatus(reader, filePath);
        status = assignCopyParquetTasks<T>(
            reader.get(), fileBlockInfos.at(filePath).startOffset, filePath, pkIndex);
        throwCopyExceptionIfNotOK(status);
    }
    return arrow::Status::OK();
}

template<typename T>
void CopyNodeArrow::populatePKIndex(BMBackedInMemColumn* pkColumn, HashIndexBuilder<T>* pkIndex,
    offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if (pkColumn->isNullAtNodeOffset(offset)) {
            throw ReaderException("Primary key cannot be null.");
        }
        appendPKIndex(pkColumn, offset, pkIndex);
    }
}

template<typename T1, typename T2>
arrow::Status CopyNodeArrow::batchPopulateColumnsTask(uint64_t primaryKeyPropertyIdx,
    uint64_t blockIdx, uint64_t startOffset, HashIndexBuilder<T1>* pkIndex, CopyNodeArrow* copier,
    const std::vector<std::shared_ptr<T2>>& batchColumns, std::string filePath) {
    copier->logger->trace("Start: path={0} blkIdx={1}", filePath, blockIdx);
    auto numLinesInCurBlock = copier->fileBlockInfos.at(filePath).numLinesPerBlock[blockIdx];

    // Pin each page within the [StartOffset, endOffset] range.
    auto endOffset = startOffset + numLinesInCurBlock - 1;
    for (auto& column : copier->columns) {
        column->pinPagesForNodeOffsets(startOffset, endOffset);
    }
    std::vector<PageByteCursor> overflowCursors(copier->tableSchema->getNumProperties());
    for (auto blockOffset = 0u; blockOffset < numLinesInCurBlock; ++blockOffset) {
        putPropsOfLineIntoColumns(copier->columns, overflowCursors, batchColumns,
            startOffset + blockOffset, blockOffset, copier->copyDescription);
    }
    // Flush each page within the [StartOffset, endOffset] range.
    for (auto& column : copier->columns) {
        column->flushPagesForNodeOffsetRange(startOffset, endOffset);
    }

    populatePKIndex(
        copier->columns[primaryKeyPropertyIdx].get(), pkIndex, startOffset, numLinesInCurBlock);

    // Unpin each page within the [StartOffset, endOffset] range.
    for (auto& column : copier->columns) {
        column->unpinPagesForNodeOffsetRange(startOffset, endOffset);
    }

    copier->logger->trace("End: path={0} blkIdx={1}", filePath, blockIdx);
    return arrow::Status::OK();
}

template<typename T>
void CopyNodeArrow::putPropsOfLineIntoColumns(
    std::vector<std::unique_ptr<BMBackedInMemColumn>>& propertyColumns,
    std::vector<PageByteCursor>& overflowCursors,
    const std::vector<std::shared_ptr<T>>& arrow_columns, uint64_t nodeOffset,
    uint64_t bufferOffset, CopyDescription& copyDescription) {
    for (auto columnIdx = 0u; columnIdx < propertyColumns.size(); columnIdx++) {
        auto column = propertyColumns[columnIdx].get();
        auto currentToken = arrow_columns[columnIdx]->GetScalar(bufferOffset);
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
                stringToken = stringToken.substr(0, BufferPoolConstants::PAGE_4KB_SIZE);
                data = stringToken.c_str();
                auto val = reinterpret_cast<BMBackedInMemColumnWithOverflow*>(column)
                               ->getInMemOverflowFile()
                               ->copyString(data, overflowCursors[columnIdx]);
                column->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&val));
            } break;
            case VAR_LIST: {
                auto varListVal = getArrowVarList(stringToken, 1, stringToken.length() - 2,
                    column->getDataType(), copyDescription);
                auto kuList = reinterpret_cast<BMBackedInMemColumnWithOverflow*>(column)
                                  ->getInMemOverflowFile()
                                  ->copyList(*varListVal, overflowCursors[columnIdx]);
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
arrow::Status CopyNodeArrow::assignCopyCSVTasks(arrow::csv::StreamingReader* csvStreamingReader,
    offset_t startOffset, std::string filePath, std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    auto it = csvStreamingReader->begin();
    auto endIt = csvStreamingReader->end();
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

template<typename T>
arrow::Status CopyNodeArrow::assignCopyParquetTasks(parquet::arrow::FileReader* parquetReader,
    common::offset_t startOffset, std::string filePath,
    std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    auto numBlocks = fileBlockInfos.at(filePath).numBlocks;
    auto blockIdx = 0u;
    std::shared_ptr<arrow::Table> currTable;
    while (blockIdx < numBlocks) {
        for (int i = 0; i < common::CopyConstants::NUM_COPIER_TASKS_TO_SCHEDULE_PER_BATCH; ++i) {
            if (blockIdx == numBlocks) {
                break;
            }
            ARROW_RETURN_NOT_OK(parquetReader->RowGroup(blockIdx)->ReadTable(&currTable));
            taskScheduler.scheduleTask(
                CopyTaskFactory::createCopyTask(batchPopulateColumnsTask<T, arrow::ChunkedArray>,
                    reinterpret_cast<NodeTableSchema*>(tableSchema)->primaryKeyPropertyID, blockIdx,
                    startOffset, pkIndex.get(), this, currTable->columns(), filePath));
            startOffset += currTable->num_rows();
            ++blockIdx;
        }
        taskScheduler.waitUntilEnoughTasksFinish(
            common::CopyConstants::MINIMUM_NUM_COPIER_TASKS_TO_SCHEDULE_MORE);
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    return arrow::Status::OK();
}

template<>
void CopyNodeArrow::appendPKIndex(
    BMBackedInMemColumn* pkColumn, common::offset_t offset, HashIndexBuilder<int64_t>* pkIndex) {
    auto element = pkColumn->getElement<int64_t>(offset);
    if (!pkIndex->append(*element, offset)) {
        throw common::CopyException(
            common::Exception::getExistedPKExceptionMsg(std::to_string(*element)));
    }
}

template<>
void CopyNodeArrow::appendPKIndex(BMBackedInMemColumn* pkColumn, common::offset_t offset,
    HashIndexBuilder<common::ku_string_t>* pkIndex) {
    auto element = pkColumn->getElement<common::ku_string_t>(offset);
    auto key = reinterpret_cast<BMBackedInMemColumnWithOverflow*>(pkColumn)
                   ->getInMemOverflowFile()
                   ->readString(element.get());
    if (!pkIndex->append(key.c_str(), offset)) {
        throw common::CopyException(common::Exception::getExistedPKExceptionMsg(key));
    }
}

} // namespace storage
} // namespace kuzu
