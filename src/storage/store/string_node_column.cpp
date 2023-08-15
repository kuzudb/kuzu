#include "storage/store/string_node_column.h"

#include "storage/copier/string_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void StringNodeColumnFunc::writeStringValuesToPage(
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    auto kuStrInFrame = (ku_string_t*)(frame + (posInFrame * sizeof(ku_string_t)));
    auto kuStrInVector = vector->getValue<ku_string_t>(posInVector);
    if (kuStrInVector.len > ku_string_t::SHORT_STR_LENGTH) {
        throw NotImplementedException("VarSizedNodeColumnFunc::writeStringValuesToPage");
    }
    memcpy(kuStrInFrame->prefix, kuStrInVector.prefix, kuStrInVector.len);
    kuStrInFrame->len = kuStrInVector.len;
}

StringNodeColumn::StringNodeColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction)
    : NodeColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, true} {
    if (this->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        writeNodeColumnFunc = StringNodeColumnFunc::writeStringValuesToPage;
    }
    overflowMetadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        StorageStructureID::newMetadataID(), metaDAHeaderInfo.childrenInfos[0]->dataDAHPageIdx,
        bufferManager, wal, transaction);
}

void StringNodeColumn::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    NodeColumn::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    auto numValuesToRead = endOffsetInGroup - startOffsetInGroup;
    auto overflowPageIdx = overflowMetadataDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    for (auto i = 0u; i < numValuesToRead; i++) {
        auto pos = offsetInVector + i;
        if (resultVector->isNull(pos)) {
            continue;
        }
        readStringValueFromOvf(
            transaction, resultVector->getValue<ku_string_t>(pos), resultVector, overflowPageIdx);
    }
}

page_idx_t StringNodeColumn::append(
    storage::ColumnChunk* columnChunk, page_idx_t startPageIdx, node_group_idx_t nodeGroupIdx) {
    auto numPagesForMainChunk = NodeColumn::append(columnChunk, startPageIdx, nodeGroupIdx);
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk);
    auto numPagesForOverflow =
        stringColumnChunk->flushOverflowBuffer(dataFH, startPageIdx + numPagesForMainChunk);
    overflowMetadataDA->resize(nodeGroupIdx + 1);
    overflowMetadataDA->update(nodeGroupIdx,
        ColumnChunkMetadata{startPageIdx + numPagesForMainChunk, numPagesForOverflow});
    return numPagesForMainChunk + numPagesForOverflow;
}

void StringNodeColumn::checkpointInMemory() {
    NodeColumn::checkpointInMemory();
    overflowMetadataDA->checkpointInMemoryIfNecessary();
}

void StringNodeColumn::rollbackInMemory() {
    NodeColumn::rollbackInMemory();
    overflowMetadataDA->rollbackInMemoryIfNecessary();
}

void StringNodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    assert(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdxFromNodeOffset(startNodeOffset);
    NodeColumn::scanInternal(transaction, nodeIDVector, resultVector);
    auto overflowPageIdx = overflowMetadataDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (resultVector->isNull(pos)) {
            continue;
        }
        readStringValueFromOvf(
            transaction, resultVector->getValue<ku_string_t>(pos), resultVector, overflowPageIdx);
    }
}

void StringNodeColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    assert(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto overflowPageIdx = overflowMetadataDA
                               ->get(StorageUtils::getNodeGroupIdxFromNodeOffset(startNodeOffset),
                                   transaction->getType())
                               .pageIdx;
    NodeColumn::lookupInternal(transaction, nodeIDVector, resultVector);
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        if (!resultVector->isNull(pos)) {
            readStringValueFromOvf(transaction, resultVector->getValue<ku_string_t>(pos),
                resultVector, overflowPageIdx);
        }
    }
}

void StringNodeColumn::readStringValueFromOvf(Transaction* transaction, ku_string_t& kuStr,
    ValueVector* resultVector, page_idx_t overflowPageIdx) {
    if (ku_string_t::isShortString(kuStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    cursor.pageIdx += overflowPageIdx;
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *dataFH, cursor.pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        StringVector::addString(
            resultVector, kuStr, (const char*)(frame + cursor.offsetInPage), kuStr.len);
    });
}

} // namespace storage
} // namespace kuzu
