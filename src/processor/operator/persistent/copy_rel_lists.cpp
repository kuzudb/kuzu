#include "processor/operator/persistent/copy_rel_lists.h"

#include "common/string_utils.h"
#include "processor/result/factorized_table.h"
#include "storage/copier/table_copy_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void CopyRelLists::executeInternal(ExecutionContext* context) {
    sharedState->logCopyRelWALRecord(info.wal);
    while (children[0]->getNextTuple(context)) {
        if (sharedState->fwdRelData->relDataFormat == RelDataFormat::CSR_LISTS) {
            copyRelLists(sharedState->fwdRelData.get(), info.boundOffsetPos, info.nbrOffsetPos,
                info.dataPoses, localState->fwdCopyStates);
        }
        if (sharedState->bwdRelData->relDataFormat == RelDataFormat::CSR_LISTS) {
            copyRelLists(sharedState->bwdRelData.get(), info.nbrOffsetPos, info.boundOffsetPos,
                info.dataPoses, localState->bwdCopyStates);
        }
    }
}

void CopyRelLists::copyRelLists(DirectedInMemRelData* relData, const DataPos& boundOffsetPos,
    const DataPos& nbrOffsetPos, const std::vector<DataPos>& dataPoses,
    std::vector<std::unique_ptr<storage::PropertyCopyState>>& copyStates) {
    auto boundOffsetChunkedArray =
        ArrowColumnVector::getArrowColumn(resultSet->getValueVector(boundOffsetPos).get());
    auto nbrOffsetChunkedArray =
        ArrowColumnVector::getArrowColumn(resultSet->getValueVector(nbrOffsetPos).get());
    assert(boundOffsetChunkedArray->num_chunks() == nbrOffsetChunkedArray->num_chunks());
    auto numChunks = boundOffsetChunkedArray->num_chunks();
    for (auto chunkIdx = 0; chunkIdx < numChunks; chunkIdx++) {
        std::vector<offset_t> posInRelLists;
        auto boundOffsetArray = boundOffsetChunkedArray->chunk(chunkIdx).get();
        auto nbrOffsetArray = nbrOffsetChunkedArray->chunk(chunkIdx).get();
        auto boundOffsets = boundOffsetArray->data()->GetValues<offset_t>(1 /* value buffer */);
        auto nbrOffsets = nbrOffsetArray->data()->GetValues<offset_t>(1 /* value buffer */);
        auto numRows = boundOffsetArray->length();
        assert(nbrOffsetArray->length() == numRows);
        posInRelLists.resize(numRows);
        for (auto i = 0u; i < numRows; i++) {
            posInRelLists[i] = InMemListsUtils::decrementListSize(
                *relData->lists->relListsSizes, boundOffsets[i], 1); // Decrement the list size.
            relData->lists->adjList->setValue(
                boundOffsets[i], posInRelLists[i], (uint8_t*)&nbrOffsets[i]);
        }
        auto posInRelListsArray = TableCopyUtils::createArrowPrimitiveArray(
            std::make_shared<arrow::Int64Type>(), (uint8_t*)posInRelLists.data(), numRows);
        // Copy Rel ID.
        // TODO: Refactor the creation of rel id array.
        std::shared_ptr<arrow::Buffer> relIDBuffer;
        TableCopyUtils::throwCopyExceptionIfNotOK(
            arrow::AllocateBuffer((int64_t)(numRows * sizeof(offset_t))).Value(&relIDBuffer));
        auto relOffsets = (offset_t*)relIDBuffer->data();
        auto offsetValueVector = resultSet->getValueVector(info.offsetPos);
        auto startRowIdx = offsetValueVector->getValue<offset_t>(
            offsetValueVector->state->selVector->selectedPositions[0]);
        for (auto rowIdx = 0u; rowIdx < numRows; rowIdx++) {
            relOffsets[rowIdx] = startRowIdx + rowIdx;
        }
        auto relIDArray = TableCopyUtils::createArrowPrimitiveArray(
            std::make_shared<arrow::Int64Type>(), relIDBuffer, boundOffsetArray->length());
        relData->lists->propertyLists.at(catalog::RelTableSchema::INTERNAL_REL_ID_PROPERTY_ID)
            ->copyArrowArray(boundOffsetArray, posInRelListsArray.get(), relIDArray.get(), nullptr);
        // Copy rel lists.
        // Skip REL_ID column, which has been copied separately.
        // Skip FROM and TO value vectors, which are used for index lookup only.
        // The mapping is as below:
        // Data value vectors: [_FROM_, _TO_, prop1, ...]
        // Columns:            [REL_ID,       prop1, ...]
        for (auto i = 2; i < dataPoses.size(); i++) {
            auto columnID = i - 1;
            auto propertyChunkedArray =
                ArrowColumnVector::getArrowColumn(resultSet->getValueVector(dataPoses[i]).get());
            relData->lists->propertyLists.at(columnID)->copyArrowArray(boundOffsetArray,
                posInRelListsArray.get(), propertyChunkedArray->chunk(chunkIdx).get(),
                copyStates[columnID].get());
        }
    }
}

void CopyRelLists::finalize(ExecutionContext* context) {
    if (sharedState->fwdRelData->relDataFormat == RelDataFormat::CSR_LISTS) {
        sharedState->fwdRelData->lists->adjList->saveToFile();
        for (auto& [_, relLists] : sharedState->fwdRelData->lists->propertyLists) {
            relLists->saveToFile();
        }
    }
    if (sharedState->bwdRelData->relDataFormat == RelDataFormat::CSR_LISTS) {
        sharedState->bwdRelData->lists->adjList->saveToFile();
        for (auto& [_, relLists] : sharedState->bwdRelData->lists->propertyLists) {
            relLists->saveToFile();
        }
    }
    sharedState->updateRelsStatistics();
    auto outputMsg = StringUtils::string_format("{} number of tuples has been copied to table: {}.",
        sharedState->numRows.load(), info.schema->tableName.c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->fTable.get(), outputMsg, context->memoryManager);
}

} // namespace processor
} // namespace kuzu
