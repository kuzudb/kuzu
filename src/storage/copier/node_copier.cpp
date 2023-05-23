#include "storage/copier/node_copier.h"

#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::unique_ptr<NodeCopyMorsel> NodeCopySharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        if (fileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[fileIdx];
        auto fileBlockInfo = fileBlockInfos.at(filePath);
        if (blockIdx >= fileBlockInfo.numBlocks) {
            // No more blocks to read in this file.
            fileIdx++;
            blockIdx = 0;
            continue;
        }
        auto result = std::make_unique<NodeCopyMorsel>(
            nodeOffset, blockIdx, fileBlockInfo.numLinesPerBlock[blockIdx], filePath);
        nodeOffset += fileBlockInfos.at(filePath).numLinesPerBlock[blockIdx];
        blockIdx++;
        return result;
    }
}

std::unique_ptr<NodeCopyMorsel> CSVNodeCopySharedState::getMorsel() {
    std::unique_lock lck{mtx};
    while (true) {
        if (fileIdx >= filePaths.size()) {
            // No more files to read.
            return nullptr;
        }
        auto filePath = filePaths[fileIdx];
        if (!reader) {
            reader = TableCopyExecutor::createCSVReader(filePath, csvReaderConfig, tableSchema);
        }
        std::shared_ptr<arrow::RecordBatch> recordBatch;
        TableCopyExecutor::throwCopyExceptionIfNotOK(reader->ReadNext(&recordBatch));
        if (recordBatch == nullptr) {
            // No more blocks to read in this file.
            fileIdx++;
            reader.reset();
            continue;
        }
        auto morselNodeOffset = nodeOffset;
        nodeOffset += recordBatch->num_rows();
        return std::make_unique<CSVNodeCopyMorsel>(
            morselNodeOffset, filePath, std::move(recordBatch));
    }
}

NodeCopier::NodeCopier(const std::string& directory,
    std::shared_ptr<NodeCopySharedState> sharedState,
    kuzu::storage::PrimaryKeyIndexBuilder* pkIndex, const common::CopyDescription& copyDesc,
    catalog::TableSchema* schema, kuzu::storage::NodeTable* table, common::column_id_t pkColumnID)
    : sharedState{std::move(sharedState)}, pkIndex{pkIndex}, copyDesc{copyDesc}, pkColumnID{
                                                                                     pkColumnID} {
    for (auto i = 0u; i < schema->properties.size(); i++) {
        auto property = schema->properties[i];
        if (property.dataType.getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            // Skip SERIAL, as it is not physically stored.
            continue;
        }
        properties.push_back(property);
        auto fPath = StorageUtils::getNodePropertyColumnFName(
            directory, schema->tableID, property.propertyID, common::DBFileType::WAL_VERSION);
        columns.push_back(std::make_shared<InMemColumn>(fPath, property.dataType));
    }
    // Each property corresponds to a column.
    assert(properties.size() == columns.size());
}

void NodeCopier::execute(processor::ExecutionContext* executionContext) {
    while (true) {
        if (executionContext->clientContext->isInterrupted()) {
            throw InterruptException();
        }
        auto morsel = sharedState->getMorsel();
        if (morsel == nullptr) {
            // No more morsels.
            break;
        }
        executeInternal(std::move(morsel));
    }
}

void NodeCopier::populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    offset_t startOffset, uint64_t numValues) {
    // First, check if there is any nulls.
    for (auto posInChunk = 0u; posInChunk < numValues; posInChunk++) {
        if (chunk->isNull(posInChunk)) {
            throw CopyException("Primary key cannot be null.");
        }
    }
    // No nulls, so we can populate the index with actual values.
    pkIndex->lock();
    switch (chunk->getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        appendToPKIndex<int64_t>(chunk, startOffset, numValues);
    } break;
    case LogicalTypeID::STRING: {
        appendToPKIndex<ku_string_t, InMemOverflowFile*>(
            chunk, startOffset, numValues, overflowFile);
    } break;
    default: {
        throw CopyException("Primary key must be either INT64, STRING or SERIAL.");
    }
    }
    pkIndex->unlock();
}

template<>
void NodeCopier::appendToPKIndex<int64_t>(
    InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<int64_t>(i);
        pkIndex->append(value, offset);
    }
}

template<>
void NodeCopier::appendToPKIndex<ku_string_t, InMemOverflowFile*>(InMemColumnChunk* chunk,
    common::offset_t startOffset, uint64_t numValues, InMemOverflowFile* overflowFile) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<ku_string_t>(offset);
        auto key = overflowFile->readString(&value);
        pkIndex->append(key.c_str(), offset);
    }
}

void NodeCopier::copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk,
    common::column_id_t columnID, arrow::Array& arrowArray, CopyDescription& copyDescription) {
    switch (arrowArray.type_id()) {
    case arrow::Type::BOOL: {
        columnChunk->templateCopyValuesToPage<bool>(arrowArray);
    } break;
    case arrow::Type::INT16: {
        columnChunk->templateCopyValuesToPage<int16_t>(arrowArray);
    } break;
    case arrow::Type::INT32: {
        columnChunk->templateCopyValuesToPage<int32_t>(arrowArray);
    } break;
    case arrow::Type::INT64: {
        columnChunk->templateCopyValuesToPage<int64_t>(arrowArray);
    } break;
    case arrow::Type::DOUBLE: {
        columnChunk->templateCopyValuesToPage<double_t>(arrowArray);
    } break;
    case arrow::Type::FLOAT: {
        columnChunk->templateCopyValuesToPage<float_t>(arrowArray);
    } break;
    case arrow::Type::DATE32: {
        columnChunk->templateCopyValuesToPage<date_t>(arrowArray);
    } break;
    case arrow::Type::TIMESTAMP: {
        columnChunk->templateCopyValuesToPage<timestamp_t>(arrowArray);
    } break;
    case arrow::Type::STRING: {
        columnChunk->templateCopyValuesToPage<std::string, InMemOverflowFile*, PageByteCursor&,
            CopyDescription&>(arrowArray, columns[columnID]->getInMemOverflowFile(),
            overflowCursors[columnID], copyDescription);
    } break;
    default: {
        throw CopyException("Unsupported data type " + arrowArray.type()->ToString());
    }
    }
}

void NodeCopier::flushChunksAndPopulatePKIndex(
    const std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks,
    common::offset_t startNodeOffset, common::offset_t endNodeOffset,
    common::column_id_t columnToFlush) {
    // Flush each page within the [StartOffset, endOffset] range.
    if (columnToFlush == INVALID_COLUMN_ID) {
        for (auto i = 0u; i < properties.size(); i++) {
            columns[i]->flushChunk(columnChunks[i].get());
        }
    } else {
        columns[columnToFlush]->flushChunk(columnChunks[0].get());
    }
    if (pkIndex) {
        // Populate the primary key index.
        populatePKIndex(columnChunks[pkColumnID].get(), columns[pkColumnID]->getInMemOverflowFile(),
            startNodeOffset, (endNodeOffset - startNodeOffset + 1));
    }
}

void CSVNodeCopier::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    auto csvMorsel = dynamic_cast<CSVNodeCopyMorsel*>(morsel.get());
    auto numLinesInCurBlock = csvMorsel->recordBatch->num_rows();
    // Create a column chunk for tuples within the [StartOffset, endOffset] range.
    auto endOffset = csvMorsel->nodeOffset + numLinesInCurBlock - 1;
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(properties.size());
    for (auto i = 0; i < properties.size(); i++) {
        auto property = properties[i];
        columnChunks[i] =
            std::make_unique<InMemColumnChunk>(property.dataType, csvMorsel->nodeOffset, endOffset);
        copyArrayIntoColumnChunk(
            columnChunks[i].get(), i, *csvMorsel->recordBatch->column(i), copyDesc);
    }
    flushChunksAndPopulatePKIndex(columnChunks, csvMorsel->nodeOffset, endOffset);
}

void ParquetNodeCopier::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    assert(!morsel->filePath.empty());
    if (!reader || filePath != morsel->filePath) {
        reader = TableCopyExecutor::createParquetReader(morsel->filePath);
    }
    std::shared_ptr<arrow::Table> table;
    TableCopyExecutor::throwCopyExceptionIfNotOK(
        reader->RowGroup(static_cast<int>(morsel->blockIdx))->ReadTable(&table));
    arrow::TableBatchReader batchReader(*table);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyExecutor::throwCopyExceptionIfNotOK(batchReader.ReadNext(&recordBatch));
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(properties.size());
    auto numLinesInCurBlock = recordBatch->num_rows();
    auto endOffset = morsel->nodeOffset + numLinesInCurBlock - 1;
    for (auto i = 0; i < properties.size(); i++) {
        auto property = properties[i];
        columnChunks[i] =
            std::make_unique<InMemColumnChunk>(property.dataType, morsel->nodeOffset, endOffset);
        NodeCopier::copyArrayIntoColumnChunk(
            columnChunks[i].get(), i, *recordBatch->column(i), copyDesc);
    }
    flushChunksAndPopulatePKIndex(columnChunks, morsel->nodeOffset, endOffset);
}

void NPYNodeCopier::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    if (!reader || reader->getFilePath() != morsel->filePath) {
        reader = std::make_unique<NpyReader>(morsel->filePath);
    }
    auto endNodeOffset = morsel->nodeOffset + morsel->numNodes - 1;
    // For NPY files, we can only read one column at a time.
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(1);
    columnChunks[0] = std::make_unique<InMemColumnChunk>(
        properties[columnToCopy].dataType, morsel->nodeOffset, endNodeOffset);
    for (auto i = 0u; i < morsel->numNodes; i++) {
        columnChunks[0]->setValue(reader->getPointerToRow(morsel->nodeOffset + i), i);
    }
    flushChunksAndPopulatePKIndex(columnChunks, morsel->nodeOffset, endNodeOffset, columnToCopy);
}

} // namespace storage
} // namespace kuzu
