#include "storage/store/string_node_column.h"

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
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal)
    : NodeColumn{
          std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, true} {
    ovfPageIdxInChunk = ColumnChunk::getNumPagesForBytes(
        numBytesPerFixedSizedValue << StorageConstants::NODE_GROUP_SIZE_LOG2);
    if (this->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        writeNodeColumnFunc = StringNodeColumnFunc::writeStringValuesToPage;
    }
}

void StringNodeColumn::scan(transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInGroup,
    common::offset_t endOffsetInGroup, common::ValueVector* resultVector, uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    NodeColumn::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    auto numValuesToRead = endOffsetInGroup - startOffsetInGroup;
    auto chunkStartPageIdx = metadataDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    for (auto i = 0u; i < numValuesToRead; i++) {
        auto pos = offsetInVector + i;
        if (resultVector->isNull(pos)) {
            continue;
        }
        readStringValueFromOvf(
            transaction, resultVector->getValue<ku_string_t>(pos), resultVector, chunkStartPageIdx);
    }
}

void StringNodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    assert(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(startNodeOffset);
    auto chunkStartPageIdx = metadataDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    NodeColumn::scanInternal(transaction, nodeIDVector, resultVector);
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        if (resultVector->isNull(pos)) {
            continue;
        }
        readStringValueFromOvf(
            transaction, resultVector->getValue<ku_string_t>(pos), resultVector, chunkStartPageIdx);
    }
}

void StringNodeColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    assert(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(startNodeOffset);
    auto chunkStartPageIdx = metadataDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    NodeColumn::lookupInternal(transaction, nodeIDVector, resultVector);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    if (!resultVector->isNull(pos)) {
        readStringValueFromOvf(
            transaction, resultVector->getValue<ku_string_t>(pos), resultVector, chunkStartPageIdx);
    }
}

void StringNodeColumn::readStringValueFromOvf(Transaction* transaction, ku_string_t& kuStr,
    ValueVector* resultVector, page_idx_t chunkStartPageIdx) {
    if (ku_string_t::isShortString(kuStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    cursor.pageIdx += (ovfPageIdxInChunk + chunkStartPageIdx);
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
