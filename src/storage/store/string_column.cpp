#include "storage/store/string_column.h"

#include "storage/store/null_column.h"
#include "storage/store/string_column_chunk.h"
#include <bit>
#include <span>

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StringColumn::StringColumn(std::string name, std::unique_ptr<LogicalType> dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
    RWPropertyStats stats, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, stats, enableCompression, true /* requireNullColumn */} {
    auto dataColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::DATA, "");
    dataColumn = std::make_unique<Column>(dataColName, LogicalType::UINT8(),
        *metaDAHeaderInfo.childrenInfos[0], dataFH, metadataFH, bufferManager, wal, transaction,
        stats, false /*enableCompression*/, false /*requireNullColumn*/);
    auto offsetColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "");
    offsetColumn = std::make_unique<Column>(offsetColName, LogicalType::UINT64(),
        *metaDAHeaderInfo.childrenInfos[1], dataFH, metadataFH, bufferManager, wal, transaction,
        stats, enableCompression, false /*requireNullColumn*/);
}

void StringColumn::scanOffsets(Transaction* transaction, const ReadState& state,
    string_offset_t* offsets, uint64_t index, uint64_t numValues, uint64_t dataSize) {
    // We either need to read the next value, or store the maximum string offset at the end.
    // Otherwise we won't know what the length of the last string is.
    if (index + numValues < state.metadata.numValues) {
        offsetColumn->scan(transaction, state, index, index + numValues + 1, (uint8_t*)offsets);
    } else {
        offsetColumn->scan(transaction, state, index, index + numValues, (uint8_t*)offsets);
        offsets[numValues] = dataSize;
    }
}

void StringColumn::scanValueToVector(Transaction* transaction, const ReadState& dataState,
    string_offset_t startOffset, string_offset_t endOffset, ValueVector* resultVector,
    uint64_t offsetInVector) {
    KU_ASSERT(endOffset >= startOffset);
    // Add string to vector first and read directly into the vector
    auto& kuString =
        StringVector::reserveString(resultVector, offsetInVector, endOffset - startOffset);
    dataColumn->scan(transaction, dataState, startOffset, endOffset, (uint8_t*)kuString.getData());
    // Update prefix to match the scanned string data
    if (!ku_string_t::isShortString(kuString.len)) {
        memcpy(kuString.prefix, kuString.getData(), ku_string_t::PREFIX_LENGTH);
    }
}

void StringColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    scanUnfiltered(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
}

void StringColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    Column::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
    if (columnChunk->getNumValues() == 0) {
        return;
    }
    auto stringColumnChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(columnChunk);

    auto dataMetadata = dataColumn->getMetadata(nodeGroupIdx, transaction->getType());
    // Make sure that the chunk is large enough
    if (dataMetadata.numValues > stringColumnChunk->getDataChunk()->getCapacity()) {
        stringColumnChunk->getDataChunk()->resize(std::bit_ceil(dataMetadata.numValues));
    }
    dataColumn->scan(transaction, nodeGroupIdx, stringColumnChunk->getDataChunk());

    auto offsetMetadata = dataColumn->getMetadata(nodeGroupIdx, transaction->getType());
    // Make sure that the chunk is large enough
    if (offsetMetadata.numValues > stringColumnChunk->getOffsetChunk()->getCapacity()) {
        stringColumnChunk->getOffsetChunk()->resize(std::bit_ceil(offsetMetadata.numValues));
    }
    offsetColumn->scan(transaction, nodeGroupIdx, stringColumnChunk->getOffsetChunk());
}

void StringColumn::append(ColumnChunk* columnChunk, node_group_idx_t nodeGroupIdx) {
    Column::append(columnChunk, nodeGroupIdx);
    auto stringColumnChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(columnChunk);
    dataColumn->append(stringColumnChunk->getDataChunk(), nodeGroupIdx);
    offsetColumn->append(stringColumnChunk->getOffsetChunk(), nodeGroupIdx);
}

void StringColumn::writeValue(const ColumnChunkMetadata& chunkMeta, node_group_idx_t nodeGroupIdx,
    offset_t offsetInChunk, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto& kuStr = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
    // Write string data to end of dataColumn
    auto startOffset =
        dataColumn->appendValues(nodeGroupIdx, (const uint8_t*)kuStr.getData(), kuStr.len);
    // Write offset
    string_index_t index =
        offsetColumn->appendValues(nodeGroupIdx, (const uint8_t*)&startOffset, 1);
    // Write index to main column
    Column::writeValue(chunkMeta, nodeGroupIdx, offsetInChunk, (uint8_t*)&index);
}

void StringColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    dataColumn->checkpointInMemory();
    offsetColumn->checkpointInMemory();
}

void StringColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    dataColumn->rollbackInMemory();
    offsetColumn->rollbackInMemory();
}

void StringColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto startOffsetInGroup =
        startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, nodeGroupIdx, startOffsetInGroup,
            startOffsetInGroup + nodeIDVector->state->selVector->selectedSize, resultVector);
    } else {
        scanFiltered(transaction, nodeGroupIdx, startOffsetInGroup, nodeIDVector, resultVector);
    }
}

void StringColumn::scanUnfiltered(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInGroup, offset_t endOffsetInGroup,
    common::ValueVector* resultVector, sel_t startPosInVector) {
    auto numValuesToRead = endOffsetInGroup - startOffsetInGroup;
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);
    Column::scan(
        transaction, indexState, startOffsetInGroup, endOffsetInGroup, (uint8_t*)indices.get());

    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < numValuesToRead; i++) {
        if (!resultVector->isNull(startPosInVector + i)) {
            offsetsToScan.emplace_back(indices[i], startPosInVector + i);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    scanValuesToVector(transaction, nodeGroupIdx, offsetsToScan, resultVector, indexState);
}

std::span<std::pair<StringColumn::string_index_t, uint64_t>> getNextRange(
    std::vector<std::pair<StringColumn::string_index_t, uint64_t>>& data, uint64_t startPos,
    uint64_t numValuesPerPage) {
    auto endPos = startPos + 1;
    auto lastOffsetToScan = data[endPos - 1].first;
    while (endPos < data.size() &&
           // Include gaps of up to one page
           data[endPos].first <= lastOffsetToScan + numValuesPerPage) {
        endPos++;
        lastOffsetToScan = data[endPos - 1].first;
    }
    return std::span(&*data.begin() + startPos, &*data.begin() + endPos);
}

void StringColumn::scanValuesToVector(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    std::vector<std::pair<string_index_t, uint64_t>>& offsetsToScan, ValueVector* resultVector,
    const ReadState& indexState) {
    auto offsetState = offsetColumn->getReadState(transaction->getType(), nodeGroupIdx);
    auto dataState = dataColumn->getReadState(transaction->getType(), nodeGroupIdx);

    auto comp = [](const auto& pair1, const auto& pair2) { return pair1.first < pair2.first; };
    auto hasHighDuplication =
        ((double)offsetState.metadata.numValues / indexState.metadata.numValues) <= 0.5;
    // If at least 50% of strings are duplicated, sort the offsets so we can re-use scanned
    // strings, and scan offsets in chunks of adjacent pages. Values which are less than one page
    // apart are scanned together. Otherwise, offsets are not sorted and are scanned in a single
    // chunk since likely that the time spent sorting will outweigh the time saved by
    // de-duplication.
    if (hasHighDuplication) {
        std::sort(&*offsetsToScan.begin(), &*offsetsToScan.end(), comp);
    }
    std::vector<string_offset_t> offsets;
    auto numOffsetsPerPage = offsetState.metadata.compMeta.numValues(
        BufferPoolConstants::PAGE_4KB_SIZE, *offsetColumn->getDataType());
    auto startPos = 0u;
    do {
        auto chunkToScan = std::span(&*offsetsToScan.begin(), &*offsetsToScan.end());
        // Note that the list will contain duplicates when indices are duplicated.
        // Each distinct value is scanned once, and re-used when writing to each output value
        offset_t numOffsetsToScan, chunkStartOffset;
        if (hasHighDuplication) {
            chunkToScan = getNextRange(offsetsToScan, startPos, numOffsetsPerPage);
            numOffsetsToScan = chunkToScan.back().first - chunkToScan.front().first + 1;
            chunkStartOffset = chunkToScan.begin()->first;
        } else {
            // If there isn't much duplication, just scan the entire range of offsets at once
            // The outer loop will only execute once.
            const auto& [min, max] =
                std::minmax_element(chunkToScan.begin(), chunkToScan.end(), comp);
            numOffsetsToScan = max->first - min->first + 1;
            chunkStartOffset = min->first;
        }
        // One extra offset to scan for the end offset of the last string
        if (numOffsetsToScan + 1 > offsets.size()) {
            offsets.resize(numOffsetsToScan + 1);
        }
        auto chunkSize = chunkToScan.size();
        scanOffsets(transaction, offsetState, offsets.data(), chunkStartOffset, numOffsetsToScan,
            dataState.metadata.numValues);

        while (!chunkToScan.empty()) {
            auto& iter = chunkToScan.front();
            auto startOffset = offsets[iter.first - chunkStartOffset];
            auto endOffset = offsets[iter.first - chunkStartOffset + 1];
            scanValueToVector(
                transaction, dataState, startOffset, endOffset, resultVector, iter.second);
            auto& scannedString = resultVector->getValue<ku_string_t>(iter.second);
            // For each string which has the same index in the dictionary as the one we scanned,
            // copy the scanned string to its position in the result vector
            chunkToScan = chunkToScan.subspan(1);
            while (chunkToScan.size() > 0 && chunkToScan.begin()->first == iter.first) {
                resultVector->setValue<ku_string_t>(chunkToScan.begin()->second, scannedString);
                chunkToScan = chunkToScan.subspan(1);
            }
        }
        startPos += chunkSize;
    } while (startPos < offsetsToScan.size());
}

void StringColumn::scanFiltered(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInGroup,
    common::ValueVector* nodeIDVector, common::ValueVector* resultVector) {

    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);

    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (!resultVector->isNull(pos)) {
            // TODO(bmwinger): optimize index scans by grouping them when adjacent
            auto offsetInGroup = startOffsetInGroup + pos;
            string_index_t index;
            Column::scan(
                transaction, indexState, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    scanValuesToVector(transaction, nodeGroupIdx, offsetsToScan, resultVector, indexState);
}

void StringColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(dataType->getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);

    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (!nodeIDVector->isNull(pos)) {
            auto offsetInGroup =
                startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx) + pos;
            string_index_t index;
            Column::scan(
                transaction, indexState, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    scanValuesToVector(transaction, nodeGroupIdx, offsetsToScan, resultVector, indexState);
}

bool StringColumn::canCommitInPlace(transaction::Transaction* transaction,
    common::node_group_idx_t nodeGroupIdx, LocalVectorCollection* localChunk,
    const offset_to_row_idx_t& insertInfo, const offset_to_row_idx_t& updateInfo) {
    std::vector<row_idx_t> rowIdxesToRead;
    for (auto& [nodeOffset, rowIdx] : updateInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    for (auto& [nodeOffset, rowIdx] : insertInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    std::sort(rowIdxesToRead.begin(), rowIdxesToRead.end());
    auto totalStringLengthToAdd = 0u;
    uint64_t newStrings = rowIdxesToRead.size();

    for (auto rowIdx : rowIdxesToRead) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);

        auto kuStr = localVector->getVector()->getValue<ku_string_t>(offsetInVector);
        totalStringLengthToAdd += kuStr.len;
    }

    // Make sure there is sufficient space in the data chunk (not currently compressed)
    auto dataColumnMetadata = getDataColumn()->getMetadata(nodeGroupIdx, transaction->getType());
    auto totalStringDataAfterUpdate = dataColumnMetadata.numValues + totalStringLengthToAdd;
    if (totalStringDataAfterUpdate >
        dataColumnMetadata.numPages * BufferPoolConstants::PAGE_4KB_SIZE) {
        // Data cannot be updated in place
        return false;
    }

    // Check if offsets can be updated in-place
    auto offsetColumnMetadata =
        getOffsetColumn()->getMetadata(nodeGroupIdx, transaction->getType());
    auto offsetCapacity =
        offsetColumnMetadata.compMeta.numValues(
            BufferPoolConstants::PAGE_4KB_SIZE, *getOffsetColumn()->getDataType()) *
        offsetColumnMetadata.numPages;
    auto totalStringsAfterUpdate = offsetColumnMetadata.numValues + newStrings;

    if (totalStringsAfterUpdate > offsetCapacity ||
        !offsetColumnMetadata.compMeta.canUpdateInPlace(
            (const uint8_t*)&totalStringDataAfterUpdate, 0, dataType->getPhysicalType())) {
        return false;
    }

    // Check if the index column can store the largest new index in-place
    auto indexColumnMetadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (!indexColumnMetadata.compMeta.canUpdateInPlace(
            (const uint8_t*)&totalStringsAfterUpdate, 0, dataType->getPhysicalType())) {
        return false;
    }

    // Indices are limited to 32 bits but in theory could be larger than that since the offset
    // column can grow beyond the node group size.
    //
    // E.g. one big string is written first, followed by NODE_GROUP_SIZE-1 small strings,
    // which are all updated in-place many times (which may fit if the first string is large
    // enough that 2^n minus the first string's size is large enough to fit the other strings,
    // for some n.
    // 32 bits should give plenty of space for updates.
    if (offsetColumnMetadata.numValues + newStrings >
        std::numeric_limits<StringColumn::string_index_t>::max()) [[unlikely]] {
        return false;
    }
    return true;
}

} // namespace storage
} // namespace kuzu
