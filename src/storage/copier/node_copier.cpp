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

template<typename T>
void NodeCopier<T>::execute(processor::ExecutionContext* executionContext) {
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
template void NodeCopier<int64_t>::execute(processor::ExecutionContext* executionContext);
template void NodeCopier<ku_string_t>::execute(processor::ExecutionContext* executionContext);

template<typename T>
void NodeCopier<T>::populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    NullMask* nullMask, HashIndexBuilder<T>* pkIndex, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        if (nullMask->isNull(offset)) {
            throw CopyException("Primary key cannot be null.");
        }
        appendPKIndex(chunk, overflowFile, offset, pkIndex);
    }
}
template void NodeCopier<int64_t>::populatePKIndex(InMemColumnChunk* chunk,
    InMemOverflowFile* overflowFile, NullMask* nullMask, HashIndexBuilder<int64_t>* pkIndex,
    offset_t startOffset, uint64_t numValues);
template void NodeCopier<ku_string_t>::populatePKIndex(InMemColumnChunk* chunk,
    InMemOverflowFile* overflowFile, NullMask* nullMask, HashIndexBuilder<ku_string_t>* pkIndex,
    offset_t startOffset, uint64_t numValues);

template<typename T>
void NodeCopier<T>::copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk, InMemNodeColumn* column,
    arrow::Array& arrowArray, offset_t startNodeOffset, CopyDescription& copyDescription,
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
template void NodeCopier<int64_t>::copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk,
    InMemNodeColumn* column, arrow::Array& arrowArray, offset_t startNodeOffset,
    CopyDescription& copyDescription, PageByteCursor& overflowCursor);
template void NodeCopier<ku_string_t>::copyArrayIntoColumnChunk(InMemColumnChunk* columnChunk,
    InMemNodeColumn* column, arrow::Array& arrowArray, offset_t startNodeOffset,
    CopyDescription& copyDescription, PageByteCursor& overflowCursor);

template<>
void NodeCopier<int64_t>::appendPKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    offset_t offset, HashIndexBuilder<int64_t>* pkIndex) {
    auto element = *(int64_t*)chunk->getValue(offset);
    if (!pkIndex->append(element, offset)) {
        throw CopyException(Exception::getExistedPKExceptionMsg(std::to_string(element)));
    }
}
template<>
void NodeCopier<ku_string_t>::appendPKIndex(InMemColumnChunk* chunk,
    InMemOverflowFile* overflowFile, offset_t offset, HashIndexBuilder<ku_string_t>* pkIndex) {
    auto element = *(ku_string_t*)chunk->getValue(offset);
    auto key = overflowFile->readString(&element);
    if (!pkIndex->append(key.c_str(), offset)) {
        throw CopyException(Exception::getExistedPKExceptionMsg(key));
    }
}

template<typename T>
void CSVNodeCopier<T>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    auto csvMorsel = dynamic_cast<CSVNodeCopyMorsel*>(morsel.get());
    auto numLinesInCurBlock = csvMorsel->recordBatch->num_rows();
    // Create a column chunk for tuples within the [StartOffset, endOffset] range.
    auto endOffset = csvMorsel->nodeOffset + numLinesInCurBlock - 1;
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(this->columns.size());
    for (auto i = 0u; i < this->columns.size(); i++) {
        auto column = this->columns[i];
        columnChunks[i] =
            std::make_unique<InMemColumnChunk>(column->getDataType(), csvMorsel->nodeOffset,
                endOffset, column->getNumBytesForElement(), column->getNumElementsInAPage());
        NodeCopier<T>::copyArrayIntoColumnChunk(columnChunks[i].get(), column,
            *csvMorsel->recordBatch->column(i), csvMorsel->nodeOffset, this->copyDesc,
            this->overflowCursors[i]);
    }
    // Flush each page within the [StartOffset, endOffset] range.
    for (auto i = 0u; i < this->columns.size(); i++) {
        auto column = this->columns[i];
        column->flushChunk(columnChunks[i].get(), csvMorsel->nodeOffset, endOffset);
    }
    auto pkColumn = this->columns[this->pkColumnID];
    NodeCopier<T>::populatePKIndex(columnChunks[this->pkColumnID].get(),
        pkColumn->getInMemOverflowFile(), pkColumn->getNullMask(), this->pkIndex,
        csvMorsel->nodeOffset, numLinesInCurBlock);
}
template void CSVNodeCopier<int64_t>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel);
template void CSVNodeCopier<ku_string_t>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel);

template<typename T>
void ParquetNodeCopier<T>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    assert(!morsel->filePath.empty());
    if (!reader || filePath != morsel->filePath) {
        reader = TableCopyExecutor::createParquetReader(morsel->filePath);
    }
    std::shared_ptr<arrow::Table> table;
    TableCopyExecutor::throwCopyExceptionIfNotOK(
        reader->RowGroup(morsel->blockIdx)->ReadTable(&table));
    arrow::TableBatchReader batchReader(*table);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyExecutor::throwCopyExceptionIfNotOK(batchReader.ReadNext(&recordBatch));
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(this->columns.size());
    auto numLinesInCurBlock = recordBatch->num_rows();
    auto endOffset = morsel->nodeOffset + numLinesInCurBlock - 1;
    for (auto i = 0u; i < this->columns.size(); i++) {
        auto column = this->columns[i];
        columnChunks[i] =
            std::make_unique<InMemColumnChunk>(column->getDataType(), morsel->nodeOffset, endOffset,
                column->getNumBytesForElement(), column->getNumElementsInAPage());
        NodeCopier<T>::copyArrayIntoColumnChunk(columnChunks[i].get(), column,
            *recordBatch->column(i), morsel->nodeOffset, this->copyDesc, this->overflowCursors[i]);
    }
    // Flush each page within the [StartOffset, endOffset] range.
    for (auto i = 0u; i < this->columns.size(); i++) {
        auto column = this->columns[i];
        column->flushChunk(columnChunks[i].get(), morsel->nodeOffset, endOffset);
    }
    auto pkColumn = this->columns[this->pkColumnID];
    NodeCopier<T>::populatePKIndex(columnChunks[this->pkColumnID].get(),
        pkColumn->getInMemOverflowFile(), pkColumn->getNullMask(), this->pkIndex,
        morsel->nodeOffset, numLinesInCurBlock);
}
template void ParquetNodeCopier<int64_t>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel);
template void ParquetNodeCopier<ku_string_t>::executeInternal(
    std::unique_ptr<NodeCopyMorsel> morsel);

template<typename T>
void NPYNodeCopier<T>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel) {
    if (!reader || reader->getFilePath() != morsel->filePath) {
        reader = std::make_unique<NpyReader>(morsel->filePath);
    }
    auto endNodeOffset = morsel->nodeOffset + morsel->numNodes - 1;
    auto column = this->columns[columnID];
    auto columnChunk = std::make_unique<InMemColumnChunk>(column->getDataType(), morsel->nodeOffset,
        endNodeOffset, column->getNumBytesForElement(), column->getNumElementsInAPage());
    for (auto i = morsel->nodeOffset; i <= endNodeOffset; ++i) {
        void* data = reader->getPointerToRow(i);
        column->setElementInChunk(columnChunk.get(), i, (uint8_t*)data);
    }
    column->flushChunk(columnChunk.get(), morsel->nodeOffset, endNodeOffset);
    if (this->pkColumnID != INVALID_COLUMN_ID) {
        NodeCopier<T>::populatePKIndex(columnChunk.get(), column->getInMemOverflowFile(),
            column->getNullMask(), this->pkIndex, morsel->nodeOffset, morsel->numNodes);
    }
}
template void NPYNodeCopier<int64_t>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel);
template void NPYNodeCopier<ku_string_t>::executeInternal(std::unique_ptr<NodeCopyMorsel> morsel);

} // namespace storage
} // namespace kuzu
