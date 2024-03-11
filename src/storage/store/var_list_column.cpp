#include "storage/store/var_list_column.h"

#include "storage/store/column.h"
#include "storage/store/null_column.h"
#include "storage/store/var_list_column_chunk.h"
#include <bit>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

offset_t ListOffsetInfoInStorage::getListStartOffset(uint64_t nodePos) const {
    if (numTotal == 0) {
        return 0;
    }
    return nodePos == numTotal ? getListEndOffset(nodePos - 1) :
                                 getListEndOffset(nodePos) - getListLength(nodePos);
}

offset_t ListOffsetInfoInStorage::getListEndOffset(uint64_t nodePos) const {
    auto offsetVector = offsetVectors[(nodePos) / DEFAULT_VECTOR_CAPACITY].get();
    return offsetVector->getValue<offset_t>((nodePos) % DEFAULT_VECTOR_CAPACITY);
}

uint64_t ListOffsetInfoInStorage::getListLength(uint64_t nodePos) const {
    KU_ASSERT(nodePos < sizeVectors.size() * common::DEFAULT_VECTOR_CAPACITY);
    auto sizeVector = sizeVectors[nodePos / common::DEFAULT_VECTOR_CAPACITY].get();
    return sizeVector->getValue<uint64_t>(nodePos % common::DEFAULT_VECTOR_CAPACITY);
}

VarListColumn::VarListColumn(std::string name, LogicalType dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, propertyStatistics, enableCompression, true /* requireNullColumn */} {
    auto sizeColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "");
    auto dataColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::DATA, "");
    sizeColumn = ColumnFactory::createColumn(sizeColName, *LogicalType::INT64(),
        *metaDAHeaderInfo.childrenInfos[0], dataFH, metadataFH, bufferManager, wal, transaction,
        propertyStatistics, enableCompression);
    dataColumn = ColumnFactory::createColumn(dataColName,
        *VarListType::getChildType(&this->dataType)->copy(), *metaDAHeaderInfo.childrenInfos[1],
        dataFH, metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
}

void VarListColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    // TODO(Ziyi): the current scan function requires two dynamic allocation of vectors which may be
    // a bottleneck of the scan performance. We need to further optimize this.
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    auto listOffsetInfoInStorage = getListOffsetInfoInStorage(
        transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector->state);
    offset_t listOffsetInVector =
        offsetInVector == 0 ? 0 :
                              resultVector->getValue<list_entry_t>(offsetInVector - 1).offset +
                                  resultVector->getValue<list_entry_t>(offsetInVector - 1).size;
    auto offsetToWriteListData = listOffsetInVector;
    auto numValues = endOffsetInGroup - startOffsetInGroup;
    for (auto i = 0u; i < numValues; i++) {
        auto length = listOffsetInfoInStorage.getListLength(i);
        resultVector->setValue(i + offsetInVector, list_entry_t{listOffsetInVector, length});
        listOffsetInVector += length;
    }
    ListVector::resizeDataVector(resultVector, listOffsetInVector);
    auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scan(transaction, nodeGroupIdx, listOffsetInfoInStorage.getListStartOffset(0),
        listOffsetInfoInStorage.getListStartOffset(numValues), dataVector, offsetToWriteListData);
}

void VarListColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    kuzu::storage::ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        Column::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
        auto sizeColumnChunk =
            ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(columnChunk)->getSizeColumnChunk();
        sizeColumn->scan(transaction, nodeGroupIdx, sizeColumnChunk, startOffset, endOffset);
        auto varListColumnChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(columnChunk);
        auto startVarListOffset = varListColumnChunk->getListStartOffset(0);
        auto endVarListOffset = varListColumnChunk->getListStartOffset(columnChunk->getNumValues());
        auto numElements = endVarListOffset - startVarListOffset;
        varListColumnChunk->resizeDataColumnChunk(std::bit_ceil(numElements));
        dataColumn->scan(transaction, nodeGroupIdx, varListColumnChunk->getDataColumnChunk(),
            startVarListOffset, endVarListOffset);
        varListColumnChunk->resetOffset();
    }
}

void VarListColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    resultVector->resetAuxiliaryBuffer();
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto startNodeOffsetInGroup =
        startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    KU_ASSERT(resultVector->state);
    auto listOffsetInfoInStorage =
        getListOffsetInfoInStorage(transaction, nodeGroupIdx, startNodeOffsetInGroup,
            startNodeOffsetInGroup + nodeIDVector->state->getOriginalSize(), resultVector->state);
    if (resultVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, nodeGroupIdx, resultVector, listOffsetInfoInStorage);
    } else {
        scanFiltered(transaction, nodeGroupIdx, resultVector, listOffsetInfoInStorage);
    }
}

void VarListColumn::lookupValue(Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto nodeOffsetInGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto listEndOffset = readOffset(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto length = readSize(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto listOffset = listEndOffset - length;
    auto offsetInVector = posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, length});
    ListVector::resizeDataVector(resultVector, offsetInVector + length);
    auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scan(transaction, StorageUtils::getNodeGroupIdx(nodeOffset), listOffset,
        listOffset + length, dataVector, offsetInVector);
}

void VarListColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    auto varListColumnChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(columnChunk);
    Column::append(varListColumnChunk, nodeGroupIdx);
    auto sizeColumnChunk = varListColumnChunk->getSizeColumnChunk();
    sizeColumn->append(sizeColumnChunk, nodeGroupIdx);
    auto dataColumnChunk = varListColumnChunk->getDataColumnChunk();
    dataColumn->append(dataColumnChunk, nodeGroupIdx);
}

void VarListColumn::scanUnfiltered(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ValueVector* resultVector, const ListOffsetInfoInStorage& listOffsetInfoInStorage) {
    auto numValuesToScan = resultVector->state->selVector->selectedSize;
    offset_t offsetInVector = 0;
    for (auto i = 0u; i < numValuesToScan; i++) {
        auto listLen = listOffsetInfoInStorage.getListLength(i);
        resultVector->setValue(i, list_entry_t{offsetInVector, listLen});
        offsetInVector += listLen;
    }
    ListVector::resizeDataVector(resultVector, offsetInVector);
    auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(0);
    auto endListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(numValuesToScan);
    auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scan(transaction, nodeGroupIdx, startListOffsetInStorage, endListOffsetInStorage,
        dataVector, 0 /* offsetInVector */);
}

void VarListColumn::scanFiltered(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ValueVector* resultVector, const ListOffsetInfoInStorage& listOffsetInfoInStorage) {
    offset_t listOffset = 0;
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        auto listLen = listOffsetInfoInStorage.getListLength(pos);
        resultVector->setValue(pos, list_entry_t{(offset_t)listOffset, (uint64_t)listLen});
        listOffset += listLen;
    }
    ListVector::resizeDataVector(resultVector, listOffset);
    listOffset = 0;
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        auto startOffsetInStorageToScan = listOffsetInfoInStorage.getListStartOffset(pos);
        auto endOffsetInStorageToScan = listOffsetInfoInStorage.getListStartOffset(pos + 1);
        auto dataVector = ListVector::getDataVector(resultVector);
        dataColumn->scan(transaction, nodeGroupIdx, startOffsetInStorageToScan,
            endOffsetInStorageToScan, dataVector, listOffset);
        listOffset += resultVector->getValue<list_entry_t>(pos).size;
    }
}

void VarListColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    sizeColumn->checkpointInMemory();
    dataColumn->checkpointInMemory();
}

void VarListColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    sizeColumn->rollbackInMemory();
    dataColumn->rollbackInMemory();
}

offset_t VarListColumn::readOffset(
    Transaction* transaction, node_group_idx_t nodeGroupIdx, offset_t offsetInNodeGroup) {
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInNodeGroup,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType));
    pageCursor.pageIdx += chunkMeta.pageIdx;
    offset_t value;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readToPageFunc(frame, pageCursor, (uint8_t*)&value, 0 /* posInVector */,
            1 /* numValuesToRead */, chunkMeta.compMeta);
    });
    return value;
}

uint64_t VarListColumn::readSize(
    Transaction* transaction, node_group_idx_t nodeGroupIdx, offset_t offsetInNodeGroup) {
    auto chunkMeta = sizeColumn->getMetadataDA()->get(nodeGroupIdx, transaction->getType());
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInNodeGroup,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType));
    pageCursor.pageIdx += chunkMeta.pageIdx;
    uint64_t value;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readToPageFunc(frame, pageCursor, (uint8_t*)&value, 0 /* posInVector */,
            1 /* numValuesToRead */, chunkMeta.compMeta);
    });
    return value;
}

ListOffsetInfoInStorage VarListColumn::getListOffsetInfoInStorage(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInNodeGroup, offset_t endOffsetInNodeGroup,
    const std::shared_ptr<DataChunkState>& state) {
    auto numOffsetsToRead = endOffsetInNodeGroup - startOffsetInNodeGroup;
    auto numOffsetVectors = numOffsetsToRead / DEFAULT_VECTOR_CAPACITY +
                            (numOffsetsToRead % DEFAULT_VECTOR_CAPACITY ? 1 : 0);
    std::vector<std::unique_ptr<ValueVector>> offsetVectors;
    std::vector<std::unique_ptr<ValueVector>> sizeVectors;
    offsetVectors.reserve(numOffsetVectors);
    sizeVectors.reserve(numOffsetVectors);
    uint64_t numOffsetsRead = 0;
    for (auto i = 0u; i < numOffsetVectors; i++) {
        auto offsetVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
        auto sizeVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
        auto numOffsetsToReadInCurBatch =
            std::min(numOffsetsToRead - numOffsetsRead, DEFAULT_VECTOR_CAPACITY);
        offsetVector->setState(state);
        sizeVector->setState(state);
        Column::scan(transaction, nodeGroupIdx, startOffsetInNodeGroup + numOffsetsRead,
            startOffsetInNodeGroup + numOffsetsRead + numOffsetsToReadInCurBatch,
            offsetVector.get(), 0 /* offsetInVector */);
        sizeColumn->scan(transaction, nodeGroupIdx, startOffsetInNodeGroup + numOffsetsRead,
            startOffsetInNodeGroup + numOffsetsRead + numOffsetsToReadInCurBatch, sizeVector.get(),
            0 /* offsetInVector */);
        offsetVectors.push_back(std::move(offsetVector));
        sizeVectors.push_back(std::move(sizeVector));
        numOffsetsRead += numOffsetsToReadInCurBatch;
    }
    return {numOffsetsToRead, std::move(offsetVectors), std::move(sizeVectors)};
}

} // namespace storage
} // namespace kuzu
