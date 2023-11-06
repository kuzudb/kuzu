#include "storage/store/string_column.h"

#include "common/type_utils.h"
#include "storage/store/string_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

struct StringColumnFunc {
    static void writeStringValuesToPage(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& /*metadata*/) {
        auto kuStrInFrame = (ku_string_t*)(frame + (posInFrame * sizeof(ku_string_t)));
        auto kuStrInVector = vector->getValue<ku_string_t>(posInVector);
        memcpy(kuStrInFrame->prefix, kuStrInVector.prefix,
            std::min((uint64_t)kuStrInVector.len, ku_string_t::SHORT_STR_LENGTH));
        kuStrInFrame->len = kuStrInVector.len;
        kuStrInFrame->overflowPtr = kuStrInVector.overflowPtr;
    }
};

StringColumn::StringColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats stats)
    : Column{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, stats, false /* enableCompression */, true /* requireNullColumn */} {
    if (this->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        writeFromVectorFunc = StringColumnFunc::writeStringValuesToPage;
    }
    overflowMetadataDA = std::make_unique<InMemDiskArray<OverflowColumnChunkMetadata>>(*metadataFH,
        DBFileID::newMetadataFileID(), metaDAHeaderInfo.childrenInfos[0]->dataDAHPageIdx,
        bufferManager, wal, transaction);
}

void StringColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    Column::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
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

void StringColumn::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    Column::scan(nodeGroupIdx, columnChunk);
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk);
    auto overflowMetadata = overflowMetadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    auto inMemOverflowFile = stringColumnChunk->getOverflowFile();
    inMemOverflowFile->addNewPages(overflowMetadata.numPages);
    for (auto i = 0u; i < overflowMetadata.numPages; i++) {
        auto pageIdx = overflowMetadata.pageIdx + i;
        FileUtils::readFromFile(dataFH->getFileInfo(), inMemOverflowFile->getPage(i)->data,
            BufferPoolConstants::PAGE_4KB_SIZE, pageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

void StringColumn::append(ColumnChunk* columnChunk, node_group_idx_t nodeGroupIdx) {
    Column::append(columnChunk, nodeGroupIdx);
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk);
    auto startPageIdx = dataFH->addNewPages(stringColumnChunk->getOverflowFile()->getNumPages());
    auto numPagesForOverflow = stringColumnChunk->flushOverflowBuffer(dataFH, startPageIdx);
    overflowMetadataDA->resize(nodeGroupIdx + 1);
    overflowMetadataDA->update(
        nodeGroupIdx, OverflowColumnChunkMetadata{startPageIdx, numPagesForOverflow,
                          stringColumnChunk->getLastOffsetInPage()});
}

void StringColumn::writeValue(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto& kuStr = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
    if (!ku_string_t::isShortString(kuStr.len)) {
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        auto overflowMetadata = overflowMetadataDA->get(nodeGroupIdx, TransactionType::WRITE);
        auto overflowPageIdxInChunk = overflowMetadata.numPages - 1;
        auto walPageIdxAndFrame = DBFileUtils::createWALVersionIfNecessaryAndPinPage(
            overflowMetadata.pageIdx + overflowPageIdxInChunk, false /* insertingNewPage */,
            *dataFH, dbFileID, *bufferManager, *wal);
        memcpy(walPageIdxAndFrame.frame + overflowMetadata.lastOffsetInPage,
            reinterpret_cast<uint8_t*>(kuStr.overflowPtr), kuStr.len);
        bufferManager->unpin(*wal->fileHandle, walPageIdxAndFrame.pageIdxInWAL);
        dataFH->releaseWALPageIdxLock(walPageIdxAndFrame.originalPageIdx);
        TypeUtils::encodeOverflowPtr(
            kuStr.overflowPtr, overflowPageIdxInChunk, overflowMetadata.lastOffsetInPage);
        overflowMetadata.lastOffsetInPage += kuStr.len;
        overflowMetadataDA->update(nodeGroupIdx, overflowMetadata);
    }
    Column::writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

void StringColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    overflowMetadataDA->checkpointInMemoryIfNecessary();
}

void StringColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    overflowMetadataDA->rollbackInMemoryIfNecessary();
}

void StringColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    Column::scanInternal(transaction, nodeIDVector, resultVector);
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

void StringColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto overflowPageIdx =
        overflowMetadataDA
            ->get(StorageUtils::getNodeGroupIdx(startNodeOffset), transaction->getType())
            .pageIdx;
    Column::lookupInternal(transaction, nodeIDVector, resultVector);
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        if (!resultVector->isNull(pos)) {
            readStringValueFromOvf(transaction, resultVector->getValue<ku_string_t>(pos),
                resultVector, overflowPageIdx);
        }
    }
}

void StringColumn::readStringValueFromOvf(Transaction* transaction, ku_string_t& kuStr,
    ValueVector* resultVector, page_idx_t overflowPageIdx) {
    if (ku_string_t::isShortString(kuStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    cursor.pageIdx += overflowPageIdx;
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, cursor.pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        StringVector::addString(
            resultVector, kuStr, (const char*)(frame + cursor.offsetInPage), kuStr.len);
    });
}

} // namespace storage
} // namespace kuzu
