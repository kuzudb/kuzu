#include "storage/copier/node_copier.h"

#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::unique_ptr<NodeCopyMorsel> NodeCopySharedState::getMorsel() {
    std::unique_lock lck{this->mtx};
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
    std::unique_lock lck{this->mtx};
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
        if (recordBatch == NULL) {
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
    NullMask* nullMask, offset_t startOffset, uint64_t numValues) {
    // First, check if there is any nulls.
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if (nullMask->isNull(offset)) {
            throw CopyException("Primary key cannot be null.");
        }
    }
    // No nulls, so we can populate the index with actual values.
    switch (chunk->getDataType().typeID) {
    case INT64: {
        appendToPKIndex<int64_t>(chunk, startOffset, numValues);
    } break;
    case STRING: {
        appendToPKIndex<ku_string_t, InMemOverflowFile*>(
            chunk, startOffset, numValues, overflowFile);
    } break;
    default: {
        throw CopyException("Primary key must be either INT64 or STRING.");
    }
    }
}

template<>
void NodeCopier::appendToPKIndex<int64_t>(
    kuzu::storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = *(int64_t*)chunk->getValue(offset);
        pkIndex->append(value, offset);
    }
}

template<>
void NodeCopier::appendToPKIndex<ku_string_t, InMemOverflowFile*>(
    kuzu::storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues,
    kuzu::storage::InMemOverflowFile* overflowFile) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = *(ku_string_t*)chunk->getValue(offset);
        auto key = overflowFile->readString(&value);
        pkIndex->append(key.c_str(), offset);
    }
}

void NodeCopier::copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk,
    common::column_id_t columnID, arrow::Array& arrowArray, offset_t startNodeOffset,
    CopyDescription& copyDescription) {
    uint64_t numValuesLeftToCopy = arrowArray.length();
    auto column = columns[columnID];
    auto& overflowCursor = overflowCursors[columnID];
    while (numValuesLeftToCopy > 0) {
        auto posInArray = arrowArray.length() - numValuesLeftToCopy;
        auto offset = startNodeOffset + posInArray;
        if (column->getDataType().typeID == common::STRUCT) {
            auto numValuesToCopy =
                std::min(numValuesLeftToCopy, reinterpret_cast<InMemStructColumnChunk*>(columnChunk)
                                                  ->getMinNumValuesLeftOnPage(offset));
            columnChunk->templateCopyValuesToPage<std::string, CopyDescription&, common::offset_t>(
                PageElementCursor{}, arrowArray, posInArray, numValuesToCopy, copyDescription,
                offset);
            numValuesLeftToCopy -= numValuesToCopy;
            continue;
        }
        auto pageCursor =
            CursorUtils::getPageElementCursor(offset, column->getNumElementsInAPage());
        auto numValuesToCopy = std::min(
            numValuesLeftToCopy, column->getNumElementsInAPage() - pageCursor.elemPosInPage);
        for (auto i = 0u; i < numValuesToCopy; i++) {
            column->setNull(startNodeOffset + posInArray + i,
                arrowArray.IsNull(static_cast<int64_t>(posInArray + i)));
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
            columnChunk->templateCopyValuesToPage<date_t>(
                pageCursor, arrowArray, posInArray, numValuesToCopy);
        } break;
        case arrow::Type::TIMESTAMP: {
            columnChunk->templateCopyValuesToPage<timestamp_t>(
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

void NodeCopier::flushChunksAndPopulatePKIndex(
    const std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks,
    common::offset_t startNodeOffset, common::offset_t endNodeOffset) {
    // Flush each page within the [StartOffset, endOffset] range.
    for (auto i = 0u; i < this->columns.size(); i++) {
        auto column = this->columns[i];
        column->flushChunk(columnChunks[i].get(), startNodeOffset, endNodeOffset);
    }
    // Populate the primary key index.
    if (!this->pkIndex) {
        return;
    }
    auto pkColumn = this->columns[this->pkColumnID];
    populatePKIndex(columnChunks[this->pkColumnID].get(), pkColumn->getInMemOverflowFile(),
        pkColumn->getNullMask(), startNodeOffset, (endNodeOffset - startNodeOffset + 1));
}

void CSVNodeCopier::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    auto csvMorsel = dynamic_cast<CSVNodeCopyMorsel*>(morsel.get());
    auto numLinesInCurBlock = csvMorsel->recordBatch->num_rows();
    // Create a column chunk for tuples within the [StartOffset, endOffset] range.
    auto endOffset = csvMorsel->nodeOffset + numLinesInCurBlock - 1;
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(this->columns.size());
    for (auto i = 0; i < this->columns.size(); i++) {
        auto column = this->columns[i];
        columnChunks[i] = InMemColumnChunkFactory::getInMemColumnChunk(column->getDataType(),
            csvMorsel->nodeOffset, endOffset, column->getNumBytesForElement(),
            column->getNumElementsInAPage());
        copyArrayIntoColumnChunk(columnChunks[i].get(), i, *csvMorsel->recordBatch->column(i),
            csvMorsel->nodeOffset, this->copyDesc);
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
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(this->columns.size());
    auto numLinesInCurBlock = recordBatch->num_rows();
    auto endOffset = morsel->nodeOffset + numLinesInCurBlock - 1;
    for (auto i = 0; i < this->columns.size(); i++) {
        auto column = this->columns[i];
        columnChunks[i] =
            std::make_unique<InMemColumnChunk>(column->getDataType(), morsel->nodeOffset, endOffset,
                column->getNumBytesForElement(), column->getNumElementsInAPage());
        NodeCopier::copyArrayIntoColumnChunk(
            columnChunks[i].get(), i, *recordBatch->column(i), morsel->nodeOffset, this->copyDesc);
    }
    flushChunksAndPopulatePKIndex(columnChunks, morsel->nodeOffset, endOffset);
}

void NPYNodeCopier::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    if (!reader || reader->getFilePath() != morsel->filePath) {
        reader = std::make_unique<NpyReader>(morsel->filePath);
    }
    auto endNodeOffset = morsel->nodeOffset + morsel->numNodes - 1;
    // For NPY files, we can only read one column at a time.
    assert(this->columns.size() == 1);
    auto column = this->columns[0];
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(1);
    columnChunks[0] = std::make_unique<InMemColumnChunk>(column->getDataType(), morsel->nodeOffset,
        endNodeOffset, column->getNumBytesForElement(), column->getNumElementsInAPage());
    for (auto i = morsel->nodeOffset; i <= endNodeOffset; ++i) {
        void* data = reader->getPointerToRow(i);
        column->setElementInChunk(columnChunks[0].get(), i, (uint8_t*)data);
    }
    flushChunksAndPopulatePKIndex(columnChunks, morsel->nodeOffset, endNodeOffset);
}

} // namespace storage
} // namespace kuzu
