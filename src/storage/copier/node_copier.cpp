#include "storage/copier/node_copier.h"

#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

static column_id_t getPKColumnID(
    const std::vector<Property>& properties, property_id_t pkPropertyID) {
    column_id_t pkColumnID = 0;
    for (auto& property : properties) {
        if (property.propertyID == pkPropertyID) {
            break;
        }
        pkColumnID++;
    }
    return pkColumnID;
}

NodeCopier::NodeCopier(const std::string& directory, std::shared_ptr<CopySharedState> sharedState,
    const CopyDescription& copyDesc, TableSchema* schema, tuple_idx_t numTuples,
    column_id_t columnToCopy)
    : sharedState{std::move(sharedState)}, copyDesc{copyDesc}, columnToCopy{columnToCopy},
      pkColumnID{INVALID_COLUMN_ID} {
    for (auto i = 0u; i < schema->properties.size(); i++) {
        auto property = schema->properties[i];
        if (property.dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            // Skip SERIAL, as it is not physically stored.
            continue;
        }
        properties.push_back(property);
        auto fPath = StorageUtils::getNodePropertyColumnFName(
            directory, schema->tableID, property.propertyID, DBFileType::ORIGINAL);
        columns.push_back(std::make_shared<InMemColumn>(fPath, property.dataType));
    }
    // Each property corresponds to a column.
    assert(properties.size() == columns.size());
    initializeIndex(directory, schema, numTuples);
}

void NodeCopier::initializeIndex(
    const std::string& directory, TableSchema* schema, tuple_idx_t numTuples) {
    auto primaryKey = reinterpret_cast<NodeTableSchema*>(schema)->getPrimaryKey();
    if (primaryKey.dataType.getLogicalTypeID() == LogicalTypeID::SERIAL ||
        (columnToCopy != INVALID_COLUMN_ID &&
            primaryKey.propertyID != schema->properties[columnToCopy].propertyID)) {
        return;
    }
    pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(
        StorageUtils::getNodeIndexFName(directory, schema->tableID, DBFileType::ORIGINAL),
        primaryKey.dataType);
    pkIndex->bulkReserve(numTuples);
    pkColumnID = getPKColumnID(schema->properties, primaryKey.propertyID);
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

void NodeCopier::finalize() {
    if (pkIndex) {
        pkIndex->flush();
    }
    for (auto& column : columns) {
        column->saveToFile();
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
    InMemColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<int64_t>(i);
        pkIndex->append(value, offset);
    }
}

template<>
void NodeCopier::appendToPKIndex<ku_string_t, InMemOverflowFile*>(InMemColumnChunk* chunk,
    offset_t startOffset, uint64_t numValues, InMemOverflowFile* overflowFile) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<ku_string_t>(offset);
        auto key = overflowFile->readString(&value);
        pkIndex->append(key.c_str(), offset);
    }
}

void NodeCopier::flushChunksAndPopulatePKIndex(
    const std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks, offset_t startNodeOffset,
    offset_t endNodeOffset) {
    // Flush each page within the [StartOffset, endOffset] range.
    if (columnToCopy == INVALID_COLUMN_ID) {
        for (auto i = 0u; i < properties.size(); i++) {
            columns[i]->flushChunk(columnChunks[i].get());
        }
    } else {
        columns[columnToCopy]->flushChunk(columnChunks[0].get());
    }
    if (pkIndex) {
        // Populate the primary key index.
        populatePKIndex(columnChunks[pkColumnID].get(), columns[pkColumnID]->getInMemOverflowFile(),
            startNodeOffset, (endNodeOffset - startNodeOffset + 1));
    }
}

void CSVNodeCopier::executeInternal(std::unique_ptr<CopyMorsel> morsel) {
    auto csvMorsel = dynamic_cast<CSVCopyMorsel*>(morsel.get());
    auto numLinesInCurBlock = csvMorsel->recordBatch->num_rows();
    // Create a column chunk for tuples within the [StartOffset, endOffset] range.
    auto endOffset = csvMorsel->tupleIdx + numLinesInCurBlock - 1;
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(properties.size());
    for (auto i = 0; i < properties.size(); i++) {
        columnChunks[i] =
            columns[i]->getInMemColumnChunk(csvMorsel->tupleIdx, endOffset, &copyDesc);
        columnChunks[i]->copyArrowArray(*csvMorsel->recordBatch->column(i));
    }
    flushChunksAndPopulatePKIndex(columnChunks, csvMorsel->tupleIdx, endOffset);
}

void ParquetNodeCopier::executeInternal(std::unique_ptr<CopyMorsel> morsel) {
    assert(!morsel->filePath.empty());
    if (!reader || filePath != morsel->filePath) {
        reader = TableCopyUtils::createParquetReader(morsel->filePath);
        filePath = morsel->filePath;
    }
    std::shared_ptr<arrow::Table> table;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        reader->RowGroup(static_cast<int>(morsel->blockIdx))->ReadTable(&table));
    arrow::TableBatchReader batchReader(*table);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyUtils::throwCopyExceptionIfNotOK(batchReader.ReadNext(&recordBatch));
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(properties.size());
    auto numLinesInCurBlock = recordBatch->num_rows();
    auto endOffset = morsel->tupleIdx + numLinesInCurBlock - 1;
    for (auto i = 0; i < properties.size(); i++) {
        columnChunks[i] = columns[i]->getInMemColumnChunk(morsel->tupleIdx, endOffset, &copyDesc);
        columnChunks[i]->copyArrowArray(*recordBatch->column(i));
    }
    flushChunksAndPopulatePKIndex(columnChunks, morsel->tupleIdx, endOffset);
}

void NPYNodeCopier::executeInternal(std::unique_ptr<CopyMorsel> morsel) {
    if (!reader || reader->getFilePath() != morsel->filePath) {
        reader = std::make_unique<NpyReader>(morsel->filePath);
    }
    auto endNodeOffset = morsel->tupleIdx + morsel->numTuples - 1;
    // For NPY files, we can only read one column at a time.
    std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks(1);
    columnChunks[0] =
        columns[columnToCopy]->getInMemColumnChunk(morsel->tupleIdx, endNodeOffset, &copyDesc);
    auto batch = reader->readBlock(morsel->blockIdx);
    columnChunks[0]->copyArrowBatch(batch);
    for (auto i = 0u; i < morsel->numTuples; i++) {
        columnChunks[0]->setValueAtPos(reader->getPointerToRow(morsel->tupleIdx + i), i);
    }
    flushChunksAndPopulatePKIndex(columnChunks, morsel->tupleIdx, endNodeOffset);
}

void NodeCopyTask::run() {
    mtx.lock();
    auto clonedNodeCopier = nodeCopier->clone();
    mtx.unlock();
    clonedNodeCopier->execute(executionContext);
}

} // namespace storage
} // namespace kuzu
