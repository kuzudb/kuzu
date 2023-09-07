#include "processor/operator/persistent/copy_rel_columns.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void CopyRelColumns::executeInternal(ExecutionContext* context) {
    common::row_idx_t numRows = 0;
    while (children[0]->getNextTuple(context)) {
        row_idx_t numFwdRows, numBwdRows;
        if (info.schema->isSingleMultiplicityInDirection(RelDataDirection::FWD)) {
            numFwdRows = copyRelColumns(RelDataDirection::FWD, sharedState->fwdRelData.get(), info,
                localState->fwdCopyStates);
        } else {
            numFwdRows = countRelLists(sharedState->fwdRelData.get(), info.boundOffsetPos);
        }
        if (info.schema->isSingleMultiplicityInDirection(RelDataDirection::BWD)) {
            numBwdRows = copyRelColumns(RelDataDirection::BWD, sharedState->bwdRelData.get(), info,
                localState->bwdCopyStates);
        } else {
            numBwdRows = countRelLists(sharedState->bwdRelData.get(), info.nbrOffsetPos);
        }
        assert(numFwdRows == numBwdRows);
        numRows += numFwdRows;
    }
    sharedState->incrementNumRows(numRows);
}

row_idx_t CopyRelColumns::copyRelColumns(RelDataDirection relDirection,
    DirectedInMemRelData* relData, const CopyRelInfo& info,
    std::vector<std::unique_ptr<storage::PropertyCopyState>>& copyStates) {
    auto boundOffsetPos = relDirection == FWD ? info.boundOffsetPos : info.nbrOffsetPos;
    auto nbrOffsetPos = relDirection == FWD ? info.nbrOffsetPos : info.boundOffsetPos;
    auto boundOffsetChunkedArray =
        ArrowColumnVector::getArrowColumn(resultSet->getValueVector(boundOffsetPos).get());
    auto nbrOffsetChunkedArray =
        ArrowColumnVector::getArrowColumn(resultSet->getValueVector(nbrOffsetPos).get());
    assert(boundOffsetChunkedArray->num_chunks() == nbrOffsetChunkedArray->num_chunks());
    auto numArrayChunks = nbrOffsetChunkedArray->num_chunks();
    auto nbrColumnChunk = relData->columns->adjColumnChunk.get();
    row_idx_t numRows_ = 0;
    // Copy RelID and adjColumn.
    for (auto i = 0; i < numArrayChunks; i++) {
        auto boundOffsetArray = boundOffsetChunkedArray->chunk(i).get();
        checkViolationOfUniqueness(relDirection, boundOffsetArray, nbrColumnChunk);
        auto nbrOffsetArray = nbrOffsetChunkedArray->chunk(i).get();
        assert(boundOffsetArray->length() == nbrOffsetArray->length());
        auto numRowsInChunk = boundOffsetArray->length();
        nbrColumnChunk->copyArrowArray(*nbrOffsetArray, nullptr /* copyState */, boundOffsetArray);
        // RelID.
        std::shared_ptr<arrow::Buffer> relIDBuffer;
        TableCopyUtils::throwCopyExceptionIfNotOK(
            arrow::AllocateBuffer((int64_t)(numRowsInChunk * sizeof(offset_t)))
                .Value(&relIDBuffer));
        auto relOffsets = (offset_t*)relIDBuffer->data();
        auto offsetValueVector = resultSet->getValueVector(info.offsetPos);
        auto startRowIdx = offsetValueVector->getValue<offset_t>(
            offsetValueVector->state->selVector->selectedPositions[0]);
        for (auto rowIdx = 0u; rowIdx < numRowsInChunk; rowIdx++) {
            relOffsets[rowIdx] = startRowIdx + rowIdx;
        }
        auto relIDArray = TableCopyUtils::createArrowPrimitiveArray(
            std::make_shared<arrow::Int64Type>(), relIDBuffer, boundOffsetArray->length());
        relData->columns->propertyColumnChunks
            .at(catalog::RelTableSchema::INTERNAL_REL_ID_PROPERTY_ID)
            ->copyArrowArray(*relIDArray, nullptr, boundOffsetChunkedArray->chunk(i).get());
        numRows_ += numRowsInChunk;
    }
    // Copy rel columns.
    // Skip REL_ID column, which has been copied separately.
    // Skip FROM and TO value vectors, which are used for index lookup only.
    // The mapping is as below:
    // Data value vectors: [_FROM_, _TO_, prop1, ...]
    // Columns:            [REL_ID,       prop1, ...]
    for (auto i = 2; i < info.dataPoses.size(); i++) {
        auto columnID = i - 1;
        auto dataPos = info.dataPoses[i];
        auto propertyChunkedArray =
            ArrowColumnVector::getArrowColumn(resultSet->getValueVector(dataPos).get());
        assert(propertyChunkedArray->num_chunks() == numArrayChunks);
        assert(relData->columns->propertyColumnChunks.contains(columnID));
        for (auto chunkIdx = 0; chunkIdx < numArrayChunks; chunkIdx++) {
            relData->columns->propertyColumnChunks.at(columnID)->copyArrowArray(
                *propertyChunkedArray->chunk(chunkIdx), copyStates[columnID].get(),
                boundOffsetChunkedArray->chunk(chunkIdx).get());
        }
    }
    return numRows_;
}

row_idx_t CopyRelColumns::countRelLists(
    DirectedInMemRelData* relData, const DataPos& boundOffsetPos) {
    auto boundOffsetChunkedArray =
        ArrowColumnVector::getArrowColumn(resultSet->getValueVector(boundOffsetPos).get());
    row_idx_t numRows_ = 0;
    for (auto& boundOffsetArray : boundOffsetChunkedArray->chunks()) {
        auto offsets = boundOffsetArray->data()->GetValues<offset_t>(1 /* value buffer */);
        for (auto i = 0u; i < boundOffsetArray->length(); i++) {
            InMemListsUtils::incrementListSize(*relData->lists->relListsSizes, offsets[i], 1);
        }
        numRows_ += boundOffsetArray->length();
    }
    return numRows_;
}

void CopyRelColumns::finalize(ExecutionContext* context) {
    if (sharedState->fwdRelData->relDataFormat == RelDataFormat::COLUMN) {
        flushRelColumns(sharedState->fwdRelData.get());
    } else {
        assert(sharedState->fwdRelData->relDataFormat == RelDataFormat::CSR_LISTS);
        buildRelListsHeaders(sharedState->fwdRelData->lists->adjList->getListHeadersBuilder().get(),
            *sharedState->fwdRelData->lists->relListsSizes);
        buildRelListsMetadata(sharedState->fwdRelData.get());
    }
    if (sharedState->bwdRelData->relDataFormat == RelDataFormat::COLUMN) {
        flushRelColumns(sharedState->bwdRelData.get());
    } else {
        assert(sharedState->bwdRelData->relDataFormat == RelDataFormat::CSR_LISTS);
        buildRelListsHeaders(sharedState->bwdRelData->lists->adjList->getListHeadersBuilder().get(),
            *sharedState->bwdRelData->lists->relListsSizes);
        buildRelListsMetadata(sharedState->bwdRelData.get());
    }
}

void CopyRelColumns::checkViolationOfUniqueness(RelDataDirection relDirection,
    arrow::Array* boundNodeOffsets, storage::InMemColumnChunk* adjColumnChunk) const {
    auto offsets = boundNodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    for (auto i = 0u; i < boundNodeOffsets->length(); i++) {
        if (!adjColumnChunk->isNull(offsets[i])) {
            throw CopyException(
                ExceptionMessage::violateUniquenessOfRelAdjColumn(info.schema->tableName,
                    getRelMultiplicityAsString(info.schema->getRelMultiplicity()),
                    std::to_string(offsets[i]),
                    RelDataDirectionUtils::relDataDirectionToString(relDirection)));
        }
        adjColumnChunk->getNullChunk()->setValue<bool>(false, offsets[i]);
    }
}

void CopyRelColumns::buildRelListsHeaders(
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

void CopyRelColumns::buildRelListsMetadata(DirectedInMemRelData* directedInMemRelData) {
    auto relListHeaders = directedInMemRelData->lists->adjList->getListHeadersBuilder().get();
    buildRelListsMetadata(directedInMemRelData->lists->adjList.get(), relListHeaders);
    for (auto& [_, propertyRelLists] : directedInMemRelData->lists->propertyLists) {
        buildRelListsMetadata(propertyRelLists.get(), relListHeaders);
    }
}

void CopyRelColumns::buildRelListsMetadata(
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

void CopyRelColumns::flushRelColumns(DirectedInMemRelData* directedInMemRelData) {
    directedInMemRelData->columns->adjColumn->flushChunk(
        directedInMemRelData->columns->adjColumnChunk.get());
    directedInMemRelData->columns->adjColumn->saveToFile();
    for (auto& [propertyID, propertyColumn] : directedInMemRelData->columns->propertyColumns) {
        propertyColumn->flushChunk(
            directedInMemRelData->columns->propertyColumnChunks.at(propertyID).get());
        propertyColumn->saveToFile();
    }
}

} // namespace processor
} // namespace kuzu
