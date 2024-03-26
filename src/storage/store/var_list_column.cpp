#include "storage/store/var_list_column.h"

#include "storage/store/column.h"
#include "storage/store/null_column.h"
#include "storage/store/var_list_column_chunk.h"
#include <bit>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

offset_t ListOffsetSizeInfo::getListStartOffset(uint64_t pos) const {
    if (numTotal == 0) {
        return 0;
    }
    return pos == numTotal ? getListEndOffset(pos - 1) : getListEndOffset(pos) - getListSize(pos);
}

offset_t ListOffsetSizeInfo::getListEndOffset(uint64_t pos) const {
    if (numTotal == 0) {
        return 0;
    }
    KU_ASSERT(pos < offsetColumnChunk->getNumValues());
    return offsetColumnChunk->getValue<offset_t>(pos);
}

list_size_t ListOffsetSizeInfo::getListSize(uint64_t pos) const {
    if (numTotal == 0) {
        return 0;
    }
    KU_ASSERT(pos < sizeColumnChunk->getNumValues());
    return sizeColumnChunk->getValue<list_size_t>(pos);
}

bool ListOffsetSizeInfo::isOffsetSortedAscending(uint64_t startPos, uint64_t endPos) const {
    offset_t prevEndOffset = getListStartOffset(startPos);
    for (auto i = startPos; i < endPos; i++) {
        offset_t currentEndOffset = getListEndOffset(i);
        auto size = getListSize(i);
        prevEndOffset += size;
        if (currentEndOffset != prevEndOffset) {
            return false;
        }
    }
    return true;
}

VarListColumn::VarListColumn(std::string name, LogicalType dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, propertyStatistics, enableCompression, true /* requireNullColumn */} {
    auto sizeColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "");
    auto dataColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::DATA, "");
    sizeColumn = ColumnFactory::createColumn(sizeColName, *LogicalType::UINT32(),
        *metaDAHeaderInfo.childrenInfos[0], dataFH, metadataFH, bufferManager, wal, transaction,
        propertyStatistics, enableCompression);
    dataColumn = ColumnFactory::createColumn(dataColName,
        *VarListType::getChildType(&this->dataType)->copy(), *metaDAHeaderInfo.childrenInfos[1],
        dataFH, metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
}

void VarListColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    auto listOffsetInfoInStorage =
        getListOffsetSizeInfo(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup);
    offset_t listOffsetInVector =
        offsetInVector == 0 ? 0 :
                              resultVector->getValue<list_entry_t>(offsetInVector - 1).offset +
                                  resultVector->getValue<list_entry_t>(offsetInVector - 1).size;
    auto offsetToWriteListData = listOffsetInVector;
    auto numValues = endOffsetInGroup - startOffsetInGroup;
    KU_ASSERT(numValues >= 0);
    for (auto i = 0u; i < numValues; i++) {
        list_size_t size = listOffsetInfoInStorage.getListSize(i);
        resultVector->setValue(i + offsetInVector, list_entry_t{listOffsetInVector, size});
        listOffsetInVector += size;
    }
    ListVector::resizeDataVector(resultVector, listOffsetInVector);
    auto dataVector = ListVector::getDataVector(resultVector);
    bool isOffsetSortedAscending = listOffsetInfoInStorage.isOffsetSortedAscending(0, numValues);
    if (isOffsetSortedAscending) {
        dataColumn->scan(transaction, nodeGroupIdx, listOffsetInfoInStorage.getListStartOffset(0),
            listOffsetInfoInStorage.getListStartOffset(numValues), dataVector,
            offsetToWriteListData);
    } else {
        for (auto i = 0u; i < numValues; i++) {
            offset_t startOffset = listOffsetInfoInStorage.getListStartOffset(i);
            offset_t appendSize = listOffsetInfoInStorage.getListSize(i);
            KU_ASSERT(appendSize >= 0);
            dataColumn->scan(transaction, nodeGroupIdx, startOffset, startOffset + appendSize,
                dataVector, offsetToWriteListData);
            offsetToWriteListData += appendSize;
        }
    }
}

void VarListColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    kuzu::storage::ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        auto varListColumnChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(columnChunk);
        Column::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
        auto sizeColumnChunk = varListColumnChunk->getSizeColumnChunk();
        sizeColumn->scan(transaction, nodeGroupIdx, sizeColumnChunk, startOffset, endOffset);
        auto resizeNumValues = varListColumnChunk->getDataColumnChunk()->getNumValues();
        bool isOffsetSortedAscending = true;
        offset_t prevOffset = varListColumnChunk->getListStartOffset(0);
        for (auto i = 0u; i < columnChunk->getNumValues(); i++) {
            auto currentEndOffset = varListColumnChunk->getListEndOffset(i);
            auto appendSize = varListColumnChunk->getListSize(i);
            prevOffset += appendSize;
            if (currentEndOffset != prevOffset) {
                isOffsetSortedAscending = false;
            }
            resizeNumValues += appendSize;
        }
        if (isOffsetSortedAscending) {
            varListColumnChunk->resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
            offset_t startVarListOffset = varListColumnChunk->getListStartOffset(0);
            offset_t endVarListOffset =
                varListColumnChunk->getListStartOffset(columnChunk->getNumValues());
            dataColumn->scan(transaction, nodeGroupIdx, varListColumnChunk->getDataColumnChunk(),
                startVarListOffset, endVarListOffset);
            varListColumnChunk->resetOffset();
        } else {
            varListColumnChunk->resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
            auto tmpDataColumnChunk =
                std::make_unique<VarListDataColumnChunk>(ColumnChunkFactory::createColumnChunk(
                    *VarListType::getChildType(&this->dataType)->copy(), enableCompression,
                    std::bit_ceil(resizeNumValues)));
            auto dataVarListColumnChunk = varListColumnChunk->getDataColumnChunk();
            for (auto i = 0u; i < columnChunk->getNumValues(); i++) {
                offset_t startVarListOffset = varListColumnChunk->getListStartOffset(i);
                offset_t endVarListOffset = varListColumnChunk->getListEndOffset(i);
                dataColumn->scan(transaction, nodeGroupIdx,
                    tmpDataColumnChunk->dataColumnChunk.get(), startVarListOffset,
                    endVarListOffset);
                KU_ASSERT(endVarListOffset - startVarListOffset ==
                          tmpDataColumnChunk->dataColumnChunk->getNumValues());
                dataVarListColumnChunk->append(tmpDataColumnChunk->dataColumnChunk.get(), 0,
                    tmpDataColumnChunk->dataColumnChunk->getNumValues());
            }
            varListColumnChunk->resetOffset();
        }
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
    auto listOffsetSizeInfo = getListOffsetSizeInfo(transaction, nodeGroupIdx,
        startNodeOffsetInGroup, startNodeOffsetInGroup + nodeIDVector->state->getOriginalSize());
    if (resultVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, nodeGroupIdx, resultVector, listOffsetSizeInfo);
    } else {
        scanFiltered(transaction, nodeGroupIdx, resultVector, listOffsetSizeInfo);
    }
}

void VarListColumn::lookupValue(Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto nodeOffsetInGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto listEndOffset = readOffset(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto size = readSize(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto listStartOffset = listEndOffset - size;
    auto offsetInVector = posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, size});
    ListVector::resizeDataVector(resultVector, offsetInVector + size);
    auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scan(transaction, StorageUtils::getNodeGroupIdx(nodeOffset), listStartOffset,
        listEndOffset, dataVector, offsetInVector);
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
    ValueVector* resultVector, const ListOffsetSizeInfo& listOffsetInfoInStorage) {
    auto numValuesToScan = resultVector->state->selVector->selectedSize;
    offset_t offsetInVector = 0;
    for (auto i = 0u; i < numValuesToScan; i++) {
        auto listLen = listOffsetInfoInStorage.getListSize(i);
        resultVector->setValue(i, list_entry_t{offsetInVector, listLen});
        offsetInVector += listLen;
    }
    ListVector::resizeDataVector(resultVector, offsetInVector);
    auto dataVector = ListVector::getDataVector(resultVector);
    offsetInVector = 0;
    bool checkOffsetOrder = listOffsetInfoInStorage.isOffsetSortedAscending(0, numValuesToScan);
    if (checkOffsetOrder) {
        auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(0);
        numValuesToScan = numValuesToScan == 0 ? 0 : numValuesToScan - 1;
        auto endListOffsetInStorage = listOffsetInfoInStorage.getListEndOffset(numValuesToScan);
        dataColumn->scan(transaction, nodeGroupIdx, startListOffsetInStorage,
            endListOffsetInStorage, dataVector, 0 /* offsetInVector */);
    } else {
        for (auto i = 0u; i < numValuesToScan; i++) {
            auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(i);
            auto appendSize = listOffsetInfoInStorage.getListSize(i);
            dataColumn->scan(transaction, nodeGroupIdx, startListOffsetInStorage,
                startListOffsetInStorage + appendSize, dataVector, offsetInVector);
            offsetInVector += appendSize;
        }
    }
}

void VarListColumn::scanFiltered(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ValueVector* resultVector, const ListOffsetSizeInfo& listOffsetSizeInfo) {
    offset_t listOffset = 0;
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        auto listSize = listOffsetSizeInfo.getListSize(pos);
        resultVector->setValue(pos, list_entry_t{(offset_t)listOffset, listSize});
        listOffset += listSize;
    }
    ListVector::resizeDataVector(resultVector, listOffset);
    listOffset = 0;
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        auto startOffsetInStorageToScan = listOffsetSizeInfo.getListStartOffset(pos);
        auto appendSize = listOffsetSizeInfo.getListSize(pos);
        auto dataVector = ListVector::getDataVector(resultVector);
        dataColumn->scan(transaction, nodeGroupIdx, startOffsetInStorageToScan,
            startOffsetInStorageToScan + appendSize, dataVector, listOffset);
        listOffset += resultVector->getValue<list_entry_t>(pos).size;
    }
}

void VarListColumn::prepareCommit() {
    Column::prepareCommit();
    sizeColumn->prepareCommit();
    dataColumn->prepareCommit();
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

list_size_t VarListColumn::readSize(
    Transaction* transaction, node_group_idx_t nodeGroupIdx, offset_t offsetInNodeGroup) {
    auto chunkMeta = sizeColumn->getMetadataDA()->get(nodeGroupIdx, transaction->getType());
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

ListOffsetSizeInfo VarListColumn::getListOffsetSizeInfo(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInNodeGroup, offset_t endOffsetInNodeGroup) {
    auto numOffsetsToRead = endOffsetInNodeGroup - startOffsetInNodeGroup;
    auto offsetColumnChunk = ColumnChunkFactory::createColumnChunk(
        *common::LogicalType::INT64(), enableCompression, numOffsetsToRead);
    auto sizeColumnChunk = ColumnChunkFactory::createColumnChunk(
        *common::LogicalType::UINT32(), enableCompression, numOffsetsToRead);
    Column::scan(transaction, nodeGroupIdx, offsetColumnChunk.get(), startOffsetInNodeGroup,
        endOffsetInNodeGroup);
    sizeColumn->scan(transaction, nodeGroupIdx, sizeColumnChunk.get(), startOffsetInNodeGroup,
        endOffsetInNodeGroup);
    return {numOffsetsToRead, std::move(offsetColumnChunk), std::move(sizeColumnChunk)};
}

void VarListColumn::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, localInsertChunks,
            insertInfo, localUpdateChunks, updateInfo, deleteInfo);
    } else {
        auto columnChunk = getEmptyChunkForCommit(updateInfo.size() + insertInfo.size());
        std::vector<offset_t> dstOffsets;
        for (auto& [offsetInDstChunk, rowIdx] : updateInfo) {
            auto [chunkIdx, offsetInLocalChunk] =
                LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
            auto localUpdateChunk = localUpdateChunks[chunkIdx];
            dstOffsets.push_back(offsetInDstChunk);
            columnChunk->append(localUpdateChunk, offsetInLocalChunk, 1);
        }
        for (auto& [offsetInDstChunk, rowIdx] : insertInfo) {
            auto [chunkIdx, offsetInLocalChunk] =
                LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
            auto localInsertChunk = localInsertChunks[chunkIdx];
            dstOffsets.push_back(offsetInDstChunk);
            columnChunk->append(localInsertChunk, offsetInLocalChunk, 1);
        }
        prepareCommitForChunk(transaction, nodeGroupIdx, dstOffsets, columnChunk.get(), 0);
    }
}

void VarListColumn::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk, offset_t startSrcOffset) {
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        commitColumnChunkOutOfPlace(
            transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets, chunk, startSrcOffset);
    } else {
        // we separate the commit into three parts: offset chunk commit, size column chunk commit,
        // data column chunk
        auto varListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(chunk);
        sizeColumn->prepareCommitForChunk(transaction, nodeGroupIdx, dstOffsets,
            varListChunk->getSizeColumnChunk(), startSrcOffset);
        auto dataColumnSize =
            dataColumn->getMetadata(nodeGroupIdx, transaction->getType()).numValues;
        auto dataColumnChunk = varListChunk->getDataColumnChunk();
        auto numListsToAppend = std::min(chunk->getNumValues(), (uint64_t)dstOffsets.size());
        auto dataSize = 0u;
        auto startVarListOffset = varListChunk->getListStartOffset(startSrcOffset);
        std::vector<common::offset_t> dstOffsetsInDataColumn;
        for (auto i = 0u; i < numListsToAppend; i++) {
            for (auto j = 0u; j < varListChunk->getListSize(startSrcOffset + i); j++) {
                dstOffsetsInDataColumn.push_back(dataColumnSize + dataSize++);
            }
        }
        dataColumn->prepareCommitForChunk(
            transaction, nodeGroupIdx, dstOffsetsInDataColumn, dataColumnChunk, startVarListOffset);
        // we need to update the offset since we do not do in-place list data update but append data
        // in the end of list data column we need to plus to original data column size to get the
        // new offset
        // TODO(Jiamin): A better way is to store the offset in a offset column, just like size
        // column. Then we can reuse prepareCommitForChunk interface for offset column.
        auto offsetChunkMeta = getMetadata(nodeGroupIdx, transaction->getType());
        auto offsetColumnChunk = ColumnChunkFactory::createColumnChunk(*dataType.copy(),
            enableCompression, 1.5 * std::bit_ceil(offsetChunkMeta.numValues + dstOffsets.size()));
        Column::scan(transaction, nodeGroupIdx, offsetColumnChunk.get());
        for (auto i = 0u; i < numListsToAppend; i++) {
            auto listEndOffset = varListChunk->getListEndOffset(startSrcOffset + i);
            auto isNull = varListChunk->getNullChunk()->isNull(startSrcOffset + i);
            offsetColumnChunk->setValue<offset_t>(dataColumnSize + listEndOffset, dstOffsets[i]);
            offsetColumnChunk->getNullChunk()->setNull(dstOffsets[i], isNull);
        }
        auto offsetListChunk =
            ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(offsetColumnChunk.get());
        offsetListChunk->getSizeColumnChunk()->setNumValues(offsetColumnChunk->getNumValues());
        Column::append(offsetColumnChunk.get(), nodeGroupIdx);
    }
}

} // namespace storage
} // namespace kuzu
