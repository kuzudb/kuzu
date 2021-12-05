#include "src/storage/include/data_structure/column.h"

#include <iostream>

namespace graphflow {
namespace storage {

void Column::readValues(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    assert(nodeIDVector->dataType == NODE);
    if (nodeIDVector->isSequence) {
        auto nodeOffset = ((nodeID_t*)nodeIDVector->values)[0].offset;
        auto pageCursor = PageUtils::getPageElementCursorForOffset(nodeOffset, numElementsPerPage);
        auto sizeLeftToCopy = nodeIDVector->state->originalSize * elementSize;
        readBySequentialCopy(
            valueVector, sizeLeftToCopy, pageCursor,
            [](uint32_t i) {
                return i;
            } /*no logical-physical page mapping is required for columns*/,
            metrics);
        return;
    }
    // Case when values are at non-sequential locations in a column.
    readFromNonSequentialLocations(nodeIDVector, valueVector, metrics);
}

Literal Column::readValue(node_offset_t offset, BufferManagerMetrics& metrics) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    Literal retVal;
    auto frame = bufferManager.pin(fileHandle, cursor.idx, metrics);
    memcpy(&retVal.val, frame + mapElementPosToByteOffset(cursor.pos), elementSize);
    bufferManager.unpin(fileHandle, cursor.idx);
    return retVal;
}

void Column::readFromNonSequentialLocations(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    auto values = valueVector->values;
    auto nodeIDValues = (nodeID_t*)nodeIDVector->values;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        auto nodeOffset = nodeIDValues[pos].offset;
        auto pageCursor = PageUtils::getPageElementCursorForOffset(nodeOffset, numElementsPerPage);
        auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
        memcpy(values + pos * elementSize, frame + mapElementPosToByteOffset(pageCursor.pos),
            elementSize);
        setNULLBitsForAPos(valueVector, frame, pageCursor.pos, pos);
        bufferManager.unpin(fileHandle, pageCursor.idx);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selectedSize; i++) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            auto nodeOffset = nodeIDValues[pos].offset;
            auto pageCursor =
                PageUtils::getPageElementCursorForOffset(nodeOffset, numElementsPerPage);
            auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
            memcpy(values + pos * elementSize, frame + mapElementPosToByteOffset(pageCursor.pos),
                elementSize);
            setNULLBitsForAPos(valueVector, frame, pageCursor.pos, pos);
            bufferManager.unpin(fileHandle, pageCursor.idx);
        }
    }
}

void StringPropertyColumn::readValues(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    Column::readValues(nodeIDVector, valueVector, metrics);
    stringOverflowPages.readStringsToVector(*valueVector, metrics);
}

Literal StringPropertyColumn::readValue(node_offset_t offset, BufferManagerMetrics& metrics) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    gf_string_t gfString;
    auto frame = bufferManager.pin(fileHandle, cursor.idx, metrics);
    memcpy(&gfString, frame + mapElementPosToByteOffset(cursor.pos), sizeof(gf_string_t));
    bufferManager.unpin(fileHandle, cursor.idx);
    Literal retVal;
    retVal.strVal = stringOverflowPages.readString(gfString, metrics);
    return retVal;
}

void AdjColumn::readValues(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& resultVector, BufferManagerMetrics& metrics) {
    assert(nodeIDVector->dataType == NODE && nodeIDVector->state == resultVector->state);
    auto nodeIDValues = (nodeID_t*)nodeIDVector->values;
    if (nodeIDVector->isSequence) {
        auto pageCursor =
            PageUtils::getPageElementCursorForOffset(nodeIDValues[0].offset, numElementsPerPage);
        readNodeIDsFromSequentialPages(
            resultVector, pageCursor,
            [](uint64_t i) {
                return i;
            } /*no logical-physical page mapping is required for columns*/,
            nodeIDCompressionScheme, metrics, false /*isAdjLists*/);
    } else {
        auto numValuesToCopy =
            nodeIDVector->state->isFlat() ? 1 : nodeIDVector->state->selectedSize;
        for (auto i = 0u; i < numValuesToCopy; i++) {
            auto pos = nodeIDVector->state->isFlat() ? nodeIDVector->state->getPositionOfCurrIdx() :
                                                       nodeIDVector->state->selectedPositions[i];
            auto nodeOffset = nodeIDValues[pos].offset;
            auto pageCursor =
                PageUtils::getPageElementCursorForOffset(nodeOffset, numElementsPerPage);
            readNodeIDsFromAPage(resultVector, pos, pageCursor.idx, pageCursor.pos,
                1 /* numValuesToCopy */, nodeIDCompressionScheme, metrics, false /*isAdjLists*/);
        }
    }
}

} // namespace storage
} // namespace graphflow
