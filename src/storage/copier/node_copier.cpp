#include "storage/copier/node_copier.h"

#include "common/string_utils.h"
#include "storage/copier/copy_task.h"
#include "storage/storage_structure/in_mem_file.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

template<typename T>
std::unique_ptr<NodeCopyMorsel> CSVNodeCopySharedState<T>::getMorsel() {
    std::unique_lock lck{this->mtx};
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopier::throwCopyExceptionIfNotOK(csvStreamingReader->ReadNext(&recordBatch));
    auto morselStartOffset = this->startOffset;
    auto morselBlockIdx = this->blockIdx;
    if (recordBatch == NULL) {
        morselStartOffset = INVALID_NODE_OFFSET;
        morselBlockIdx = NodeCopyMorsel::INVALID_BLOCK_IDX;
    } else {
        this->startOffset += recordBatch->num_rows();
        this->blockIdx++;
    }
    return make_unique<NodeCopyMorsel>(morselStartOffset, morselBlockIdx, std::move(recordBatch));
}

template<typename T>
std::unique_ptr<NodeCopyMorsel> ParquetNodeCopySharedState<T>::getMorsel() {
    std::unique_lock lck{this->mtx};
    std::shared_ptr<arrow::Table> currTable;
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    if (this->blockIdx == numBlocks) {
        return make_unique<NodeCopyMorsel>(
            INVALID_NODE_OFFSET, NodeCopyMorsel::INVALID_BLOCK_IDX, std::move(recordBatch));
    }
    TableCopier::throwCopyExceptionIfNotOK(
        parquetReader->RowGroup(this->blockIdx)->ReadTable(&currTable));
    if (currTable == NULL) {
        return make_unique<NodeCopyMorsel>(
            INVALID_NODE_OFFSET, NodeCopyMorsel::INVALID_BLOCK_IDX, std::move(recordBatch));
    }
    // TODO(GUODONG): We assume here each time the table only contains one record batch. Needs to
    // verify if this always holds true.
    arrow::TableBatchReader batchReader(*currTable);
    TableCopier::throwCopyExceptionIfNotOK(batchReader.ReadNext(&recordBatch));
    auto numRows = currTable->num_rows();
    this->startOffset += numRows;
    this->blockIdx++;
    return make_unique<NodeCopyMorsel>(
        this->startOffset - numRows, this->blockIdx - 1, std::move(recordBatch));
}

void NodeCopier::initializeColumnsAndLists() {
    logger->info("Initializing in memory columns.");
    for (auto& property : tableSchema->properties) {
        auto fName = StorageUtils::getNodePropertyColumnFName(
            outputDirectory, tableSchema->tableID, property.propertyID, DBFileType::WAL_VERSION);
        columns[property.propertyID] =
            NodeInMemColumnFactory::getNodeInMemColumn(fName, property.dataType, numRows);
    }
    logger->info("Done initializing in memory columns.");
}

void NodeCopier::populateColumnsAndLists(processor::ExecutionContext* executionContext) {
    auto primaryKey = reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey();
    switch (primaryKey.dataType.typeID) {
    case INT64: {
        populateColumns<int64_t>(executionContext);
    } break;
    case STRING: {
        populateColumns<ku_string_t>(executionContext);
    } break;
    default: {
        throw CopyException(StringUtils::string_format("Unsupported data type {} for the ID index.",
            Types::dataTypeToString(primaryKey.dataType)));
    }
    }
}

void NodeCopier::saveToFile() {
    logger->debug("Writing node columns to disk.");
    assert(!columns.empty());
    for (auto& [_, column] : columns) {
        taskScheduler.scheduleTask(CopyTaskFactory::createCopyTask(
            [&](InMemNodeColumn* x) { x->saveToFile(); }, column.get()));
    }
    taskScheduler.waitAllTasksToCompleteOrError();
    logger->debug("Done writing node columns to disk.");
}

template<typename T>
void NodeCopier::populateColumns(processor::ExecutionContext* executionContext) {
    logger->info("Populating properties");
    auto pkIndex =
        std::make_unique<HashIndexBuilder<T>>(StorageUtils::getNodeIndexFName(this->outputDirectory,
                                                  tableSchema->tableID, DBFileType::WAL_VERSION),
            reinterpret_cast<NodeTableSchema*>(tableSchema)->getPrimaryKey().dataType);
    pkIndex->bulkReserve(numRows);
    switch (copyDescription.fileType) {
    case CopyDescription::FileType::CSV:
        populateColumnsFromCSV<T>(executionContext, pkIndex);
        break;
    case CopyDescription::FileType::PARQUET:
        populateColumnsFromParquet<T>(executionContext, pkIndex);
        break;
    default: {
        throw CopyException(StringUtils::string_format("Unsupported file type {}.",
            CopyDescription::getFileTypeName(copyDescription.fileType)));
    }
    }
    logger->info("Flush the pk index to disk.");
    pkIndex->flush();
    logger->info("Done populating properties, constructing the pk index.");
}

template<typename T>
void NodeCopier::populateColumnsFromCSV(
    processor::ExecutionContext* executionContext, std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    for (auto& filePath : copyDescription.filePaths) {
        std::shared_ptr<arrow::csv::StreamingReader> csvStreamingReader = initCSVReader(filePath);
        CSVNodeCopySharedState sharedState{
            filePath, pkIndex.get(), fileBlockInfos.at(filePath).startOffset, csvStreamingReader};
        logger->info("Create shared state for file {}, startOffset.", filePath,
            fileBlockInfos.at(filePath).startOffset);
        taskScheduler.scheduleTaskAndWaitOrError(
            CopyTaskFactory::createParallelCopyTask(executionContext->numThreads,
                populateColumnChunksTask<T>, &sharedState, this, executionContext, *logger),
            executionContext);
    }
}

template<typename T>
void NodeCopier::populateColumnsFromParquet(
    processor::ExecutionContext* executionContext, std::unique_ptr<HashIndexBuilder<T>>& pkIndex) {
    for (auto& filePath : copyDescription.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> parquetReader = initParquetReader(filePath);
        ParquetNodeCopySharedState sharedState{filePath, pkIndex.get(),
            fileBlockInfos.at(filePath).startOffset, fileBlockInfos.at(filePath).numBlocks,
            std::move(parquetReader)};
        taskScheduler.scheduleTaskAndWaitOrError(
            CopyTaskFactory::createParallelCopyTask(executionContext->numThreads,
                populateColumnChunksTask<T>, &sharedState, this, executionContext, *logger),
            executionContext);
    }
}

template<typename T>
void NodeCopier::populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    common::NullMask* nullMask, HashIndexBuilder<T>* pkIndex, offset_t startOffset,
    uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if (nullMask->isNull(offset)) {
            throw ReaderException("Primary key cannot be null.");
        }
        appendPKIndex(chunk, overflowFile, offset, pkIndex);
    }
}

template<typename T>
void NodeCopier::populateColumnChunksTask(NodeCopySharedState<T>* sharedState, NodeCopier* copier,
    processor::ExecutionContext* executionContext, spdlog::logger& logger) {
    while (true) {
        if (executionContext->clientContext->isInterrupted()) {
            throw common::InterruptException{};
        }
        auto result = sharedState->getMorsel();
        logger.info("Get a morsel from file {}.", sharedState->filePath);
        if (!result->success()) {
            logger.info("No more morsels from file {}.", sharedState->filePath);
            break;
        }
        auto numLinesInCurBlock = result->recordBatch->num_rows();
        // Create a column chunk for tuples within the [StartOffset, endOffset] range.
        auto endOffset = result->startOffset + numLinesInCurBlock - 1;
        logger.info("Processing a morsel from file {}: {} to {}.", sharedState->filePath,
            result->startOffset, endOffset);
        std::unordered_map<uint64_t, std::unique_ptr<InMemColumnChunk>> chunks;
        for (auto& [propertyIdx, column] : copier->columns) {
            chunks[propertyIdx] =
                std::make_unique<InMemColumnChunk>(column->getDataType(), result->startOffset,
                    endOffset, column->getNumBytesForElement(), column->getNumElementsInAPage());
        }
        std::vector<PageByteCursor> overflowCursors(copier->tableSchema->getNumProperties());
        for (auto& [propertyIdx, column] : copier->columns) {
            logger.info("copy array into column chunk for property {}.", propertyIdx);
            copyArrayIntoColumnChunk(chunks.at(propertyIdx).get(), column.get(),
                *result->recordBatch->column(propertyIdx), result->startOffset,
                copier->copyDescription, overflowCursors[propertyIdx]);
        }
        logger.info("Flush a morsel from file {}: {} to {}.", sharedState->filePath,
            result->startOffset, endOffset);
        // Flush each page within the [StartOffset, endOffset] range.
        for (auto& [propertyIdx, column] : copier->columns) {
            column->flushChunk(chunks[propertyIdx].get(), result->startOffset, endOffset);
        }
        logger.info("Populate hash index for a morsel from file {}: {} to {}.",
            sharedState->filePath, result->startOffset, endOffset);
        auto primaryKeyPropertyIdx =
            reinterpret_cast<NodeTableSchema*>(copier->tableSchema)->primaryKeyPropertyID;
        auto pkColumn = copier->columns.at(primaryKeyPropertyIdx).get();
        populatePKIndex(chunks[primaryKeyPropertyIdx].get(), pkColumn->getInMemOverflowFile(),
            pkColumn->getNullMask(), sharedState->pkIndex, result->startOffset, numLinesInCurBlock);
    }
}

void NodeCopier::copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk, InMemNodeColumn* column,
    arrow::Array& arrowArray, common::offset_t startNodeOffset, CopyDescription& copyDescription,
    PageByteCursor& overflowCursor) {
    uint64_t numValuesLeftToCopy = arrowArray.length();
    while (numValuesLeftToCopy > 0) {
        auto posInArray = arrowArray.length() - numValuesLeftToCopy;
        auto pageCursor = CursorUtils::getPageElementCursor(
            startNodeOffset + posInArray, column->getNumElementsInAPage());
        auto numValuesToCopy = std::min(
            numValuesLeftToCopy, column->getNumElementsInAPage() - pageCursor.elemPosInPage);
        for (auto i = 0u; i < numValuesToCopy; i++) {
            column->getNullMask()->setNull(
                startNodeOffset + posInArray + i, arrowArray.IsNull(posInArray + i));
        }
        switch (arrowArray.type_id()) {
        case arrow::Type::BOOL: {
            columnChunk->templateCopyValuesToPage<bool>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::INT16: {
            columnChunk->templateCopyValuesToPage<int16_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::INT32: {
            columnChunk->templateCopyValuesToPage<int32_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::INT64: {
            columnChunk->templateCopyValuesToPage<int64_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::DOUBLE: {
            columnChunk->templateCopyValuesToPage<double_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::FLOAT: {
            columnChunk->templateCopyValuesToPage<float_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::DATE32: {
            columnChunk->templateCopyValuesToPage<common::date_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::TIMESTAMP: {
            columnChunk->templateCopyValuesToPage<common::timestamp_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::STRING: {
            columnChunk->templateCopyValuesToPage<std::string, InMemOverflowFile*, PageByteCursor&,
                CopyDescription&>(pageCursor, arrowArray, posInArray, numValuesToCopy,
                column->getInMemOverflowFile(), overflowCursor, copyDescription);
        } break;
        default: {
            throw CopyException("Unsupported data type " + arrowArray.type()->ToString());
        }
        }
        numValuesLeftToCopy -= numValuesToCopy;
    }
}

template<>
void NodeCopier::appendPKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    common::offset_t offset, HashIndexBuilder<int64_t>* pkIndex) {
    auto element = *(int64_t*)chunk->getValue(offset);
    if (!pkIndex->append(element, offset)) {
        throw common::CopyException(
            common::Exception::getExistedPKExceptionMsg(std::to_string(element)));
    }
}

template<>
void NodeCopier::appendPKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    common::offset_t offset, HashIndexBuilder<common::ku_string_t>* pkIndex) {
    auto element = *(ku_string_t*)chunk->getValue(offset);
    auto key = overflowFile->readString(&element);
    if (!pkIndex->append(key.c_str(), offset)) {
        throw common::CopyException(common::Exception::getExistedPKExceptionMsg(key));
    }
}

} // namespace storage
} // namespace kuzu
