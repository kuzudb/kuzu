#include "storage/store/array_column.h"

#include <iostream>

#include "storage/store/array_column_chunk.h"
#include "storage/store/column.h"
#include "storage/store/null_column.h"
#include <bit>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

offset_t ListOffsetInfoInStorage2::getListOffset(uint64_t nodePos) const {
    if (nodePos == 0) {
        return prevNodeListOffset;
    } else {
        auto offsetVector = offsetVectors[(nodePos - 1) / DEFAULT_VECTOR_CAPACITY].get();
        return offsetVector->getValue<offset_t>((nodePos - 1) % DEFAULT_VECTOR_CAPACITY);
    }
}

ArrayColumn::ArrayColumn(std::string name, common::LogicalType dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, propertyStatistics, enableCompression, true /* requireNullColumn */} {
    auto dataColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::DATA, "");
    KU_ASSERT(metaDAHeaderInfo.childrenInfos.size() != 0);
    dataColumn = ColumnFactory::createColumn(dataColName, *LogicalType::BLOB(),
        *metaDAHeaderInfo.childrenInfos[0], dataFH, metadataFH, bufferManager, wal, transaction,
        propertyStatistics, enableCompression);
}

void ArrayColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    std::cout << "scan here 1\n";
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
    dataColumn->scan(transaction, nodeGroupIdx, listOffsetInfoInStorage.getListOffset(0),
        listOffsetInfoInStorage.getListOffset(numValues), ListVector::getDataVector(resultVector),
        offsetToWriteListData);
}

void ArrayColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    kuzu::storage::ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    std::cout << "scan here 2\n";
    auto arrayColumnChunk = ku_dynamic_cast<ColumnChunk*, ArrayColumnChunk*>(columnChunk);
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        arrayColumnChunk->setNumValues(0);
    } else {
        Column::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
        auto startVarListOffset = arrayColumnChunk->getListOffset(0);
        auto endVarListOffset = arrayColumnChunk->getListOffset(columnChunk->getNumValues());
        auto numElements = endVarListOffset - startVarListOffset + 1;
        arrayColumnChunk->resizeDataColumnChunk(std::bit_ceil(numElements));
        dataColumn->scan(transaction, nodeGroupIdx, arrayColumnChunk->getDataColumnChunk(),
            startVarListOffset, endVarListOffset);
    }
}

void ArrayColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    std::cout << "scan Internal 1\n";
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

void ArrayColumn::lookupValue(Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    std::cout << "lookupValue 1\n";
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto nodeOffsetInGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto listOffset = readListOffsetInStorage(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto length = readListOffsetInStorage(transaction, nodeGroupIdx, nodeOffsetInGroup + 1) -
                  readListOffsetInStorage(transaction, nodeGroupIdx, nodeOffsetInGroup);
    auto offsetInVector = posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, length});
    ListVector::resizeDataVector(resultVector, offsetInVector + length);
    dataColumn->scan(transaction, StorageUtils::getNodeGroupIdx(nodeOffset), listOffset,
        listOffset + length, ListVector::getDataVector(resultVector), offsetInVector);
}

// TODO(Jimain): column chunk append (use for copy Node)
void ArrayColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    std::cout << "append here 1\n";
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    Column::append(columnChunk, nodeGroupIdx);
    auto dataColumnChunk =
        ku_dynamic_cast<ColumnChunk*, ArrayColumnChunk*>(columnChunk)->getDataColumnChunk();
    dataColumn->append(dataColumnChunk, nodeGroupIdx);
}

void ArrayColumn::scanUnfiltered(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ValueVector* resultVector, const ListOffsetInfoInStorage2& listOffsetInfoInStorage) {
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
    dataColumn->scan(transaction, nodeGroupIdx, startListOffsetInStorage, endListOffsetInStorage,
        ListVector::getDataVector(resultVector), 0 /* offsetInVector */);
}

void ArrayColumn::scanFiltered(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ValueVector* resultVector, const ListOffsetInfoInStorage2& listOffsetInfoInStorage) {
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
        dataColumn->scan(transaction, nodeGroupIdx, startOffsetInStorageToScan,
            endOffsetInStorageToScan, ListVector::getDataVector(resultVector), listOffset);
        listOffset += resultVector->getValue<list_entry_t>(pos).size;
    }
}

void ArrayColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    dataColumn->checkpointInMemory();
}

void ArrayColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    dataColumn->rollbackInMemory();
}

offset_t ArrayColumn::readOffset(
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

ListOffsetInfoInStorage2 ArrayColumn::getListOffsetInfoInStorage(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInNodeGroup, offset_t endOffsetInNodeGroup,
    const std::shared_ptr<DataChunkState>& state) {
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
        Column::scan(transaction, nodeGroupIdx, startOffsetInNodeGroup + numOffsetsRead,
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
