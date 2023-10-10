#include "storage/store/var_list_node_column.h"

#include "storage/store/node_column.h"
#include "storage/store/var_list_column_chunk.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

common::offset_t ListOffsetInfoInStorage::getListOffset(uint64_t nodePos) const {
    if (nodePos == 0) {
        return prevNodeListOffset;
    } else {
        auto offsetVector = offsetVectors[(nodePos - 1) / common::DEFAULT_VECTOR_CAPACITY].get();
        return offsetVector->getValue<common::offset_t>(
            (nodePos - 1) % common::DEFAULT_VECTOR_CAPACITY);
    }
}

void VarListNodeColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
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
    dataNodeColumn->scan(transaction, nodeGroupIdx, listOffsetInfoInStorage.getListOffset(0),
        listOffsetInfoInStorage.getListOffset(numValues), ListVector::getDataVector(resultVector),
        offsetToWriteListData);
}

void VarListNodeColumn::scan(
    node_group_idx_t nodeGroupIdx, kuzu::storage::ColumnChunk* columnChunk) {
    auto varListColumnChunk = reinterpret_cast<VarListColumnChunk*>(columnChunk);
    if (nodeGroupIdx >= metadataDA->getNumElements()) {
        varListColumnChunk->setNumValues(0);
    } else {
        NodeColumn::scan(nodeGroupIdx, columnChunk);
        auto metadata = metadataDA->get(nodeGroupIdx, transaction::TransactionType::READ_ONLY);
        varListColumnChunk->setNumValues(metadata.numValues);
        varListColumnChunk->resizeDataColumnChunk(
            metadata.numPages * BufferPoolConstants::PAGE_4KB_SIZE);
        dataNodeColumn->scan(nodeGroupIdx, varListColumnChunk->getDataColumnChunk());
    }
}

void VarListNodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    resultVector->resetAuxiliaryBuffer();
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto startNodeOffsetInGroup =
        startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto listOffsetInfoInStorage =
        getListOffsetInfoInStorage(transaction, nodeGroupIdx, startNodeOffsetInGroup,
            startNodeOffsetInGroup + nodeIDVector->state->getOriginalSize(), resultVector->state);
    if (resultVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, nodeGroupIdx, resultVector, listOffsetInfoInStorage);
    } else {
        scanFiltered(transaction, nodeGroupIdx, resultVector, listOffsetInfoInStorage);
    }
}

void VarListNodeColumn::lookupValue(Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto nodeOffsetInGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto listOffset = readListOffsetInStorage(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto length = readListOffsetInStorage(transaction, nodeGroupIdx, nodeOffsetInGroup + 1) -
                  readListOffsetInStorage(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto offsetInVector = posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, length});
    ListVector::resizeDataVector(resultVector, offsetInVector + length);
    dataNodeColumn->scan(transaction, StorageUtils::getNodeGroupIdx(nodeOffset), listOffset,
        listOffset + length, ListVector::getDataVector(resultVector), offsetInVector);
}

void VarListNodeColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    NodeColumn::append(columnChunk, nodeGroupIdx);
    auto dataColumnChunk = reinterpret_cast<VarListColumnChunk*>(columnChunk)->getDataColumnChunk();
    dataNodeColumn->append(dataColumnChunk, nodeGroupIdx);
}

void VarListNodeColumn::scanUnfiltered(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ValueVector* resultVector, const ListOffsetInfoInStorage& listOffsetInfoInStorage) {
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
        endListOffsetInStorage, ListVector::getDataVector(resultVector), 0 /* offsetInVector */);
}

void VarListNodeColumn::scanFiltered(Transaction* transaction, node_group_idx_t nodeGroupIdx,
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
    Transaction* transaction, node_group_idx_t nodeGroupIdx, offset_t offsetInNodeGroup) {
    auto offsetVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
    offsetVector->state = DataChunkState::getSingleValueDataChunkState();
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    auto pageCursor = PageUtils::getPageElementCursorForPos(offsetInNodeGroup,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType));
    pageCursor.pageIdx += chunkMeta.pageIdx;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readToVectorFunc(frame, pageCursor, offsetVector.get(), 0 /* posInVector */,
            1 /* numValuesToRead */, chunkMeta.compMeta);
    });
    return offsetVector->getValue<offset_t>(0);
}

ListOffsetInfoInStorage VarListNodeColumn::getListOffsetInfoInStorage(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInNodeGroup, offset_t endOffsetInNodeGroup,
    std::shared_ptr<DataChunkState> state) {
    auto numOffsetsToRead = endOffsetInNodeGroup - startOffsetInNodeGroup;
    auto numOffsetVectors = numOffsetsToRead / DEFAULT_VECTOR_CAPACITY +
                            (numOffsetsToRead % DEFAULT_VECTOR_CAPACITY ? 1 : 0);
    std::vector<std::unique_ptr<ValueVector>> offsetVectors;
    offsetVectors.reserve(numOffsetVectors);
    uint64_t numOffsetsRead = 0;
    for (auto i = 0u; i < numOffsetVectors; i++) {
        auto offsetVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
        auto numOffsetsToReadInCurBatch =
            std::min(numOffsetsToRead - numOffsetsRead, DEFAULT_VECTOR_CAPACITY);
        offsetVector->setState(state);
        NodeColumn::scan(transaction, nodeGroupIdx, startOffsetInNodeGroup + numOffsetsRead,
            startOffsetInNodeGroup + numOffsetsRead + numOffsetsToReadInCurBatch,
            offsetVector.get(), 0 /* offsetInVector */);
        offsetVectors.push_back(std::move(offsetVector));
        numOffsetsRead += numOffsetsToReadInCurBatch;
    }
    auto prevNodeListOffsetInStorage =
        readListOffsetInStorage(transaction, nodeGroupIdx, startOffsetInNodeGroup);
    return {prevNodeListOffsetInStorage, std::move(offsetVectors)};
}

} // namespace storage
} // namespace kuzu
