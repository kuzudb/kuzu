#include "storage/copier/rel_copier.h"

#include "common/string_utils.h"
#include "storage/copier/rel_copy_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void RelCopier::execute(processor::ExecutionContext* executionContext) {
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

void RelCopier::copyRelColumnsOrCountRelListsSize(tuple_idx_t tupleIdx,
    arrow::RecordBatch* recordBatch, RelDataDirection direction,
    const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets) {
    if (schema->isSingleMultiplicityInDirection(direction)) {
        copyRelColumns(tupleIdx, recordBatch, direction, pkOffsets);
    } else {
        countRelListsSize(direction, pkOffsets);
    }
}

void RelCopier::indexLookup(arrow::Array* pkArray, const LogicalType& pkColumnType,
    PrimaryKeyIndex* pkIndex, offset_t* offsets) {
    auto length = pkArray->length();
    if (pkArray->null_count() != 0) {
        throw CopyException("Primary key column cannot contain null values.");
    }
    auto numKeysFound = 0u;
    switch (pkColumnType.getLogicalTypeID()) {
    case LogicalTypeID::SERIAL: {
        for (auto i = 0u; i < length; i++) {
            offsets[i] = static_cast<offset_t>(dynamic_cast<arrow::Int64Array*>(pkArray)->Value(i));
        }
        numKeysFound = length;
    } break;
    case LogicalTypeID::INT64: {
        for (auto i = 0u; i < length; i++) {
            auto val = dynamic_cast<arrow::Int64Array*>(pkArray)->Value(i);
            numKeysFound += pkIndex->lookup(&transaction::DUMMY_READ_TRANSACTION, val, offsets[i]);
        }
    } break;
    case LogicalTypeID::STRING: {
        for (auto i = 0u; i < length; i++) {
            auto val = std::string(dynamic_cast<arrow::StringArray*>(pkArray)->GetView(i));
            numKeysFound +=
                pkIndex->lookup(&transaction::DUMMY_READ_TRANSACTION, val.c_str(), offsets[i]);
        }
    } break;
    default: {
        throw NotImplementedException("Primary key column type not supported.");
    }
    }
    if (numKeysFound != length) {
        throw CopyException("Primary key column contains values that do not exist in the table.");
    }
}

void RelCopier::copyRelColumns(offset_t tupleIdx, arrow::RecordBatch* recordBatch,
    RelDataDirection direction, const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets) {
    auto boundPKOffsets = pkOffsets[direction == FWD ? 0 : 1].get();
    auto adjPKOffsets = pkOffsets[direction == FWD ? 1 : 0].get();
    auto relData = direction == FWD ? fwdRelData : bwdRelData;
    auto adjColumnChunk = relData->columns->adjColumnChunk.get();
    checkViolationOfRelColumn(direction, boundPKOffsets);
    adjColumnChunk->copyArrowArray(*adjPKOffsets, boundPKOffsets);
    auto numTuples = recordBatch->num_rows();
    std::vector<offset_t> relIDs;
    relIDs.resize(numTuples);
    for (auto rowIdx = 0u; rowIdx < numTuples; rowIdx++) {
        relIDs[rowIdx] = tupleIdx + rowIdx;
    }
    auto relIDArray = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)relIDs.data(), numTuples);
    relData->columns->propertyColumnChunks[0]->copyArrowArray(*relIDArray, boundPKOffsets);
    // Skip the two pk columns in arrow record batch.
    for (auto i = 2; i < recordBatch->num_columns(); i++) {
        // Skip RelID property, which is auto-generated and always the first property in table.
        relData->columns->propertyColumnChunks[i - 1]->copyArrowArray(
            *recordBatch->column(i), boundPKOffsets);
    }
}

void RelCopier::countRelListsSize(
    RelDataDirection direction, const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets) {
    auto boundPKOffsets = pkOffsets[direction == FWD ? 0 : 1].get();
    auto relData = direction == FWD ? fwdRelData : bwdRelData;
    auto offsets = boundPKOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    for (auto i = 0u; i < boundPKOffsets->length(); i++) {
        InMemListsUtils::incrementListSize(*relData->lists->relListsSizes, offsets[i], 1);
    }
}

void RelCopier::copyRelLists(offset_t tupleIdx, arrow::RecordBatch* recordBatch,
    RelDataDirection direction, const std::vector<std::unique_ptr<arrow::Array>>& pkOffsets) {
    auto boundPKOffsets = pkOffsets[direction == FWD ? 0 : 1].get();
    auto adjPKOffsets = pkOffsets[direction == FWD ? 1 : 0].get();
    auto relData = direction == FWD ? fwdRelData : bwdRelData;
    auto offsets = boundPKOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    auto adjOffsets = adjPKOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    auto numTuples = recordBatch->num_rows();
    std::vector<offset_t> posInRelLists, relIDs;
    posInRelLists.resize(numTuples);
    relIDs.resize(numTuples);
    for (auto rowIdx = 0u; rowIdx < recordBatch->num_rows(); rowIdx++) {
        posInRelLists[rowIdx] = InMemListsUtils::decrementListSize(
            *relData->lists->relListsSizes, offsets[rowIdx], 1); // Decrement the list size.
        relData->lists->adjList->setValue(
            offsets[rowIdx], posInRelLists[rowIdx], (uint8_t*)&adjOffsets[rowIdx]);
    }
    for (auto rowIdx = 0u; rowIdx < recordBatch->num_rows(); rowIdx++) {
        relIDs[rowIdx] = tupleIdx + rowIdx;
    }
    auto posInRelListsArray = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)posInRelLists.data(), numTuples);
    auto relIDArray = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)relIDs.data(), numTuples);
    relData->lists->propertyLists.at(0)->copyArrowArray(
        boundPKOffsets, posInRelListsArray.get(), relIDArray.get());
    // Skip the two pk columns in arrow record batch.
    for (auto columnIdx = 2; columnIdx < recordBatch->num_columns(); columnIdx++) {
        // Skip RelID property, which is auto-generated and always the first property in table.
        relData->lists->propertyLists.at(columnIdx - 1)
            ->copyArrowArray(
                boundPKOffsets, posInRelListsArray.get(), recordBatch->column(columnIdx).get());
    }
}

void RelCopier::checkViolationOfRelColumn(
    RelDataDirection direction, arrow::Array* boundNodeOffsets) {
    auto offsets = boundNodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    auto columnChunk = direction == FWD ? fwdRelData->columns->adjColumnChunk.get() :
                                          bwdRelData->columns->adjColumnChunk.get();
    for (auto i = 0u; i < boundNodeOffsets->length(); i++) {
        if (!columnChunk->isNull(offsets[i])) {
            throw CopyException(
                StringUtils::string_format("RelTable {} is a {} table, but node(nodeOffset: {}) "
                                           "has more than one neighbour in the {} direction.",
                    schema->tableName, getRelMultiplicityAsString(schema->relMultiplicity),
                    offsets[i], RelDataDirectionUtils::relDataDirectionToString(direction)));
        }
        columnChunk->getNullChunk()->setValue<bool>(false, offsets[i]);
    }
}

std::unique_ptr<arrow::PrimitiveArray> RelCopier::createArrowPrimitiveArray(
    const std::shared_ptr<arrow::DataType>& type, const uint8_t* data, uint64_t length) {
    auto buffer = std::make_shared<arrow::Buffer>(data, length);
    return std::make_unique<arrow::PrimitiveArray>(type, length, buffer);
}

void RelListsCounterAndColumnCopier::finalize() {
    if (fwdRelData->isColumns) {
        flushRelColumns(fwdRelData);
    } else {
        buildRelListsHeaders(fwdRelData->lists->adjList->getListHeadersBuilder().get(),
            *fwdRelData->lists->relListsSizes);
        buildRelListsMetadata(fwdRelData);
    }
    if (bwdRelData->isColumns) {
        flushRelColumns(bwdRelData);
    } else {
        buildRelListsHeaders(bwdRelData->lists->adjList->getListHeadersBuilder().get(),
            *bwdRelData->lists->relListsSizes);
        buildRelListsMetadata(bwdRelData);
    }
}

void RelListsCounterAndColumnCopier::buildRelListsMetadata(
    DirectedInMemRelData* directedInMemRelData) {
    auto relListHeaders = directedInMemRelData->lists->adjList->getListHeadersBuilder().get();
    buildRelListsMetadata(directedInMemRelData->lists->adjList.get(), relListHeaders);
    for (auto& [_, propertyRelLists] : directedInMemRelData->lists->propertyLists) {
        buildRelListsMetadata(propertyRelLists.get(), relListHeaders);
    }
}

void RelListsCounterAndColumnCopier::flushRelColumns(DirectedInMemRelData* directedInMemRelData) {
    directedInMemRelData->columns->adjColumn->flushChunk(
        directedInMemRelData->columns->adjColumnChunk.get());
    directedInMemRelData->columns->adjColumn->saveToFile();
    for (auto& [propertyID, propertyColumn] : directedInMemRelData->columns->propertyColumns) {
        propertyColumn->flushChunk(
            directedInMemRelData->columns->propertyColumnChunks.at(propertyID).get());
        propertyColumn->saveToFile();
    }
}

void RelListsCounterAndColumnCopier::buildRelListsMetadata(
    InMemLists* relLists, ListHeadersBuilder* relListHeaders) {
    auto numBoundNodes = relListHeaders->getNumValues();
    auto numChunks = StorageUtils::getNumChunks(numBoundNodes);
    offset_t nodeOffset = 0;
    auto numValuesPerPage = relLists->getNumElementsInAPage();
    for (auto chunkIdx = 0u; chunkIdx < numChunks; chunkIdx++) {
        auto numPagesForChunk = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numBoundNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numValuesInRelList = relListHeaders->getListSize(nodeOffset);
            while (numValuesInRelList + offsetInPage > numValuesPerPage) {
                numValuesInRelList -= (numValuesPerPage - offsetInPage);
                numPagesForChunk++;
                offsetInPage = 0u;
            }
            offsetInPage += numValuesInRelList;
            nodeOffset++;
        }
        if (0 != offsetInPage) {
            numPagesForChunk++;
        }
        relLists->getListsMetadataBuilder()->populateChunkPageList(
            chunkIdx, numPagesForChunk, relLists->inMemFile->getNumPages());
        relLists->inMemFile->addNewPages(numPagesForChunk);
    }
}

void RelListsCounterAndColumnCopier::buildRelListsHeaders(
    ListHeadersBuilder* relListHeadersBuilder, const atomic_uint64_vec_t& relListsSizes) {
    auto numBoundNodes = relListHeadersBuilder->getNumValues();
    auto numChunks = StorageUtils::getNumChunks(numBoundNodes);
    offset_t nodeOffset = 0;
    for (auto chunkId = 0u; chunkId < numChunks; chunkId++) {
        offset_t chunkNodeOffset = nodeOffset;
        auto numNodesInChunk = std::min(
            (offset_t)ListsMetadataConstants::LISTS_CHUNK_SIZE, numBoundNodes - nodeOffset);
        csr_offset_t csrOffset = relListsSizes[chunkNodeOffset].load(std::memory_order_relaxed);
        for (auto i = 1u; i < numNodesInChunk; i++) {
            auto currNodeOffset = chunkNodeOffset + i;
            auto numElementsInList = relListsSizes[currNodeOffset].load(std::memory_order_relaxed);
            relListHeadersBuilder->setCSROffset(currNodeOffset, csrOffset);
            csrOffset += numElementsInList;
        }
        relListHeadersBuilder->setCSROffset(chunkNodeOffset + numNodesInChunk, csrOffset);
        nodeOffset += numNodesInChunk;
    }
}

void ParquetRelListsCounterAndColumnsCopier::executeInternal(std::unique_ptr<CopyMorsel> morsel) {
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
    auto numTuples = recordBatch->num_rows();
    std::vector<offset_t> boundPKOffsets, adjPKOffsets;
    boundPKOffsets.resize(numTuples);
    adjPKOffsets.resize(numTuples);
    indexLookup(recordBatch->column(0).get(), pkColumnTypes[0], pkIndexes[0],
        (offset_t*)boundPKOffsets.data());
    indexLookup(recordBatch->column(1).get(), pkColumnTypes[1], pkIndexes[1],
        (offset_t*)adjPKOffsets.data());
    std::vector<std::unique_ptr<arrow::Array>> pkOffsetsArrays(2);
    pkOffsetsArrays[0] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)boundPKOffsets.data(), numTuples);
    pkOffsetsArrays[1] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)adjPKOffsets.data(), numTuples);
    copyRelColumnsOrCountRelListsSize(morsel->tupleIdx, recordBatch.get(), FWD, pkOffsetsArrays);
    copyRelColumnsOrCountRelListsSize(morsel->tupleIdx, recordBatch.get(), BWD, pkOffsetsArrays);
}

void CSVRelListsCounterAndColumnsCopier::executeInternal(std::unique_ptr<CopyMorsel> morsel) {
    assert(!morsel->filePath.empty());
    auto csvRelCopyMorsel = reinterpret_cast<CSVCopyMorsel*>(morsel.get());
    auto recordBatch = csvRelCopyMorsel->recordBatch;
    auto numTuples = recordBatch->num_rows();
    std::vector<offset_t> boundPKOffsets, adjPKOffsets;
    boundPKOffsets.resize(numTuples);
    adjPKOffsets.resize(numTuples);
    indexLookup(
        recordBatch->column(0).get(), pkColumnTypes[0], pkIndexes[0], boundPKOffsets.data());
    indexLookup(recordBatch->column(1).get(), pkColumnTypes[1], pkIndexes[1], adjPKOffsets.data());
    std::vector<std::unique_ptr<arrow::Array>> pkOffsets(2);
    pkOffsets[0] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)boundPKOffsets.data(), numTuples);
    pkOffsets[1] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)adjPKOffsets.data(), numTuples);
    copyRelColumnsOrCountRelListsSize(morsel->tupleIdx, recordBatch.get(), FWD, pkOffsets);
    copyRelColumnsOrCountRelListsSize(morsel->tupleIdx, recordBatch.get(), BWD, pkOffsets);
}

void RelListsCopier::finalize() {
    if (!fwdRelData->isColumns) {
        fwdRelData->lists->adjList->saveToFile();
        for (auto& [_, relLists] : fwdRelData->lists->propertyLists) {
            relLists->saveToFile();
        }
    }
    if (!bwdRelData->isColumns) {
        bwdRelData->lists->adjList->saveToFile();
        for (auto& [_, relLists] : bwdRelData->lists->propertyLists) {
            relLists->saveToFile();
        }
    }
}

void ParquetRelListsCopier::executeInternal(std::unique_ptr<CopyMorsel> morsel) {
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
    auto numTuples = recordBatch->num_rows();
    std::vector<offset_t> boundPKOffsets, adjPKOffsets;
    boundPKOffsets.resize(numTuples);
    adjPKOffsets.resize(numTuples);
    indexLookup(
        recordBatch->column(0).get(), pkColumnTypes[0], pkIndexes[0], boundPKOffsets.data());
    indexLookup(recordBatch->column(1).get(), pkColumnTypes[1], pkIndexes[1], adjPKOffsets.data());
    std::vector<std::unique_ptr<arrow::Array>> pkOffsets(2);
    pkOffsets[0] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)boundPKOffsets.data(), numTuples);
    pkOffsets[1] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)adjPKOffsets.data(), numTuples);
    if (!fwdRelData->isColumns) {
        copyRelLists(morsel->tupleIdx, recordBatch.get(), FWD, pkOffsets);
    }
    if (!bwdRelData->isColumns) {
        copyRelLists(morsel->tupleIdx, recordBatch.get(), BWD, pkOffsets);
    }
}

void CSVRelListsCopier::executeInternal(std::unique_ptr<CopyMorsel> morsel) {
    assert(!morsel->filePath.empty());
    auto csvRelCopyMorsel = reinterpret_cast<CSVCopyMorsel*>(morsel.get());
    auto recordBatch = csvRelCopyMorsel->recordBatch;
    auto numTuples = recordBatch->num_rows();
    std::vector<offset_t> boundPKOffsets, adjPKOffsets;
    boundPKOffsets.resize(numTuples);
    adjPKOffsets.resize(numTuples);
    indexLookup(
        recordBatch->column(0).get(), pkColumnTypes[0], pkIndexes[0], boundPKOffsets.data());
    indexLookup(recordBatch->column(1).get(), pkColumnTypes[1], pkIndexes[1], adjPKOffsets.data());
    std::vector<std::unique_ptr<arrow::Array>> pkOffsets(2);
    pkOffsets[0] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)boundPKOffsets.data(), numTuples);
    pkOffsets[1] = createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), (uint8_t*)adjPKOffsets.data(), numTuples);
    if (!fwdRelData->isColumns) {
        copyRelLists(morsel->tupleIdx, recordBatch.get(), FWD, pkOffsets);
    }
    if (!bwdRelData->isColumns) {
        copyRelLists(morsel->tupleIdx, recordBatch.get(), BWD, pkOffsets);
    }
}

void RelCopyTask::run() {
    mtx.lock();
    auto clonedNodeCopier = relCopier->clone();
    mtx.unlock();
    clonedNodeCopier->execute(executionContext);
}

} // namespace storage
} // namespace kuzu
