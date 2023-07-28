#include "storage/store/var_list_node_column.h"

#include "storage/copier/var_list_column_chunk.h"
#include "storage/store/node_column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void VarListNodeColumn::scan(transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInGroup,
    common::offset_t endOffsetInGroup, common::ValueVector* resultVector, uint64_t offsetInVector) {
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
    dataNodeColumn->scan(transaction, nodeGroupIdx, listOffsetInfoInStorage.getListOffset(0),
        listOffsetInfoInStorage.getListOffset(numValues), ListVector::getDataVector(resultVector),
        offsetToWriteListData);
}

void VarListNodeColumn::scanInternal(transaction::Transaction* transaction,
    common::ValueVector* nodeIDVector, common::ValueVector* resultVector) {
    resultVector->resetAuxiliaryBuffer();
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(startNodeOffset);
    auto startNodeOffsetInGroup =
        startNodeOffset - (nodeGroupIdx << common::StorageConstants::NODE_GROUP_SIZE_LOG2);
    auto listOffsetInfoInStorage =
        getListOffsetInfoInStorage(transaction, nodeGroupIdx, startNodeOffsetInGroup,
            startNodeOffsetInGroup + nodeIDVector->state->originalSize, resultVector->state);
    if (resultVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, nodeGroupIdx, resultVector, listOffsetInfoInStorage);
    } else {
        scanFiltered(transaction, nodeGroupIdx, resultVector, listOffsetInfoInStorage);
    }
}

void VarListNodeColumn::lookupValue(transaction::Transaction* transaction,
    common::offset_t nodeOffset, common::ValueVector* resultVector, uint32_t posInVector) {
    auto listOffset = readListOffsetInStorage(transaction, nodeOffset);
    auto length = readListOffsetInStorage(transaction, nodeOffset + 1) -
                  readListOffsetInStorage(transaction, nodeOffset);
    auto offsetInVector = posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, length});
    ListVector::resizeDataVector(resultVector, offsetInVector + length);
    dataNodeColumn->scan(transaction, getNodeGroupIdxFromNodeOffset(nodeOffset), listOffset,
        listOffset + length, ListVector::getDataVector(resultVector), offsetInVector);
}

page_idx_t VarListNodeColumn::append(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    auto numPagesFlushed = NodeColumn::append(columnChunk, startPageIdx, nodeGroupIdx);
    auto dataColumnChunk = reinterpret_cast<VarListColumnChunk*>(columnChunk)->getDataColumnChunk();
    auto numPagesForDataColumn =
        dataNodeColumn->append(dataColumnChunk, startPageIdx + numPagesFlushed, nodeGroupIdx);
    return numPagesFlushed + numPagesForDataColumn;
}

void VarListNodeColumn::scanUnfiltered(transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, ValueVector* resultVector,
    const ListOffsetInfoInStorage& listOffsetInfoInStorage) {
    auto numValuesToScan = resultVector->state->selVector->selectedSize;
    offset_t offsetInVector = 0;
    for (auto i = 0u; i < numValuesToScan; i++) {
        auto listLen = listOffsetInfoInStorage.getListLength(i);
        resultVector->setValue(i, list_entry_t{offsetInVector, listLen});
        offsetInVector += listLen;
    }
    ListVector::resizeDataVector(resultVector, offsetInVector);
    auto startListOffsetInStorage = listOffsetInfoInStorage.getListOffset(0);
    auto endListOffsetInStorage = listOffsetInfoInStorage.getListOffset(numValuesToScan);
    dataNodeColumn->scan(transaction, nodeGroupIdx, startListOffsetInStorage,
        endListOffsetInStorage, ListVector::getDataVector(resultVector));
}

void VarListNodeColumn::scanFiltered(transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, common::ValueVector* resultVector,
    const ListOffsetInfoInStorage& listOffsetInfoInStorage) {
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
        auto startOffsetInStorageToScan = listOffsetInfoInStorage.getListOffset(pos);
        auto endOffsetInStorageToScan = listOffsetInfoInStorage.getListOffset(pos + 1);
        dataNodeColumn->scan(transaction, nodeGroupIdx, startOffsetInStorageToScan,
            endOffsetInStorageToScan, ListVector::getDataVector(resultVector), listOffset);
        listOffset += resultVector->getValue<list_entry_t>(pos).size;
    }
}

void VarListNodeColumn::checkpointInMemory() {
    NodeColumn::checkpointInMemory();
    dataNodeColumn->checkpointInMemory();
}

void VarListNodeColumn::rollbackInMemory() {
    NodeColumn::rollbackInMemory();
    dataNodeColumn->rollbackInMemory();
}

offset_t VarListNodeColumn::readOffset(
    transaction::Transaction* transaction, common::offset_t valuePos) {
    auto offsetVector = std::make_unique<ValueVector>(common::LogicalTypeID::INT64);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(valuePos);
    offsetVector->state = DataChunkState::getSingleValueDataChunkState();
    auto pageCursor = PageUtils::getPageElementCursorForPos(valuePos, numValuesPerPage);
    pageCursor.pageIdx += metadataDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readNodeColumnFunc(
            frame, pageCursor, offsetVector.get(), 0 /* posInVector */, 1 /* numValuesToRead */);
    });
    return offsetVector->getValue<offset_t>(0);
}

ListOffsetInfoInStorage VarListNodeColumn::getListOffsetInfoInStorage(
    transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx, offset_t startOffset,
    offset_t endOffset, std::shared_ptr<DataChunkState> state) {
    auto offsetVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
    offsetVector->setState(state);
    NodeColumn::scan(transaction, nodeGroupIdx, startOffset, endOffset, offsetVector.get());
    auto prevNodeListOffsetInStorage = readListOffsetInStorage(transaction, startOffset);
    return {prevNodeListOffsetInStorage, std::move(offsetVector)};
}

} // namespace storage
} // namespace kuzu
