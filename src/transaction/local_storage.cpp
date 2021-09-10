#include "src/transaction/include/local_storage.h"

namespace graphflow {
namespace transaction {

static unique_ptr<uint8_t[]> getNullValuePtrForDataType(DataType dataType) {
    auto nullValue = make_unique<uint8_t[]>(TypeUtils::getDataTypeSize(dataType));
    switch (dataType) {
    case INT64:
        memcpy(nullValue.get(), &NULL_INT64, TypeUtils::getDataTypeSize(dataType));
        break;
    case DOUBLE:
        memcpy(nullValue.get(), &NULL_DOUBLE, TypeUtils::getDataTypeSize(dataType));
        break;
    case BOOL:
        memcpy(nullValue.get(), &NULL_BOOL, TypeUtils::getDataTypeSize(dataType));
        break;
    case DATE:
        memcpy(nullValue.get(), &NULL_DATE, TypeUtils::getDataTypeSize(dataType));
        break;
    case STRING: {
        gf_string_t nullStr;
        nullStr.set(string(&gf_string_t::EMPTY_STRING));
        memcpy(nullValue.get(), &NULL_BOOL, TypeUtils::getDataTypeSize(dataType));
        break;
    }
    default:
        assert(false);
    }
    return nullValue;
}

void LocalStorage::assignNodeIDs(label_t nodeLabel) {
    // TODO: get the list of nodeIDs from the free list, and recycle them.
    // // Since we do not have the free list implemented yet, assume that the number of recycled
    // // nodeIds is 0.
    // numRecycledIds gives the number of tuples in the collection of dataChunks that
    // will be inserted to the required columns in the order they appear. Once dealt with these
    // tuples, rest of the tuples' data is insereted sequentially in appropriate columns.
    auto numRecycledIds = 0u;
    for (auto& dataChunk : dataChunks) {
        // The last vector in the dataChunk is the outValueVector containing newly assigned nodeIDs.
        // auto outValueVector = make_shared<ValueVector>();
        // dataChunk->append(outValueVector);
        // TODO: Insert property keys in the hash index.
        // We first use the recycled nodeIDs and then assign from after `numNodes`, sequentially.
        // while (there is more nodeIDs to recycles, recycle) {
        //  }
        // handle the remaining.
    }
    uncommittedNumRecycledNodeIDs = numRecycledIds;
}

void LocalStorage::mapNodeIDs(label_t nodeLabel) {
    for (auto& dataChunk : dataChunks) {
        // The last vector in the dataChunk is the outValueVector containing newly assigned nodeIDs.
        // auto outValueVector = make_shared<ValueVector>(nodeLabel, NodeIDCompressionScheme(),
        // false); dataChunk->append(outValueVector);
        // TODO: Map property keys to nodeIDs using hash index.
    }
    uncommittedNumRecycledNodeIDs = 0;
}

void LocalStorage::deleteNodeIDs(label_t nodeLabel) {
    for (auto& dataChunk : dataChunks) {
        // TODO: delete the nodeID entry from the hash index.
        // TODO: put the deleted nodeID in the free list.
    }
    uncommittedNumRecycledNodeIDs = 0;
}

void LocalStorage::computeCreateNode(vector<uint32_t> propertyKeyIdxToVectorPosMap,
    label_t nodeLabel, vector<Column*> nodePropertyColumns, uint64_t numNodes) {
    uncommittedLabelToNumNodes.emplace(
        nodeLabel, numNodes + max((uint64_t)0ul, numTuples - uncommittedNumRecycledNodeIDs));
    for (auto i = 0u; i < propertyKeyIdxToVectorPosMap.size(); i++) {
        auto numUpdates = uncommittedNumRecycledNodeIDs;
        auto& column = *nodePropertyColumns[i];
        // Here, we are assuming that the fileHandle of the column is not already in the
        // uncommittedPages map and hence, none of its pages are dirty. That is we assume that a
        // previous update or create node has not happened to the same set of nodes that are being
        // deleted.
        uncommittedPages.emplace(column.getFileHandle(), page_idx_to_dirty_page_map());
        auto& dirtyPagesMap = uncommittedPages[column.getFileHandle()];
        auto dataChunkIdx = 0u;
        auto currPosInDataChunk = 0u;
        ValueVector* propertyValueVector = nullptr;
        auto nullValue = getNullValuePtrForDataType(column.dataType);
        while (dataChunkIdx < dataChunks.size()) {
            auto& dataChunk = *dataChunks[dataChunkIdx];
            auto nodeIDVector = dataChunk.valueVectors.back().get();
            if (-1u != propertyKeyIdxToVectorPosMap[i]) {
                propertyValueVector = dataChunk.valueVectors[propertyKeyIdxToVectorPosMap[i]].get();
            }
            if (0 < numUpdates) {
                auto numUpdatesInDataChunk = numUpdates >= dataChunk.state->selectedSize ?
                                                 dataChunk.state->selectedSize :
                                                 numUpdates;
                numUpdates -= numUpdatesInDataChunk;
                updateNodePropertyColumn(currPosInDataChunk, numUpdatesInDataChunk, column,
                    nullValue.get(), dirtyPagesMap, propertyValueVector, nodeIDVector);
                if (numUpdatesInDataChunk + currPosInDataChunk == dataChunk.state->selectedSize) {
                    // dataChunk exhausted, continue to the next dataChunk
                    dataChunkIdx++;
                    currPosInDataChunk = 0;
                    continue;
                } else {
                    currPosInDataChunk += numUpdatesInDataChunk;
                }
            }
            // If there are more created nodes that are left in the datachunk.
            if (0 == numUpdates) {
                appendToNodePropertyColumn(dataChunkIdx, currPosInDataChunk, column,
                    nullValue.get(), dirtyPagesMap, dataChunk.state->selectedSize,
                    propertyValueVector, nodeIDVector);
            }
        }
    }
    dataChunks.clear();
}

void LocalStorage::computeUpdateNode(vector<uint32_t> propertyKeyIdxToVectorPosMap,
    label_t nodeLabel, vector<Column*> nodePropertyColumns, uint64_t numNodes) {
    for (auto i = 0u; i < propertyKeyIdxToVectorPosMap.size(); i++) {
        if (-1u == propertyKeyIdxToVectorPosMap[i]) {
            continue;
        }
        auto& column = *nodePropertyColumns[i];
        // Here, we are assuming that the fileHandle of the column is not already in the
        // uncommittedPages map and hence, none of its pages are dirty. That is we assume that a
        // previous update or create node has not happened to the same set of nodes that are being
        // deleted.
        uncommittedPages.emplace(column.getFileHandle(), page_idx_to_dirty_page_map());
        auto& dirtyPagesMap = uncommittedPages[column.getFileHandle()];
        auto dataChunkIdx = 0u;
        while (dataChunkIdx < dataChunks.size()) {
            auto& dataChunk = *dataChunks[dataChunkIdx];
            auto nodeIDVector = dataChunk.valueVectors.back().get();
            auto propertyValueVector =
                dataChunk.valueVectors[propertyKeyIdxToVectorPosMap[i]].get();
            updateNodePropertyColumn(0 /*currIdx*/, dataChunk.state->selectedSize, column,
                nullptr /*nullValue pointer*/, dirtyPagesMap, propertyValueVector, nodeIDVector);
            dataChunkIdx++;
        }
    }
    dataChunks.clear();
}

void LocalStorage::computeDeleteNode(vector<uint32_t> propertyKeyIdxToVectorPosMap,
    label_t nodeLabel, vector<Column*> nodePropertyColumns, uint64_t numNodes) {
    for (auto i = 0u; i < propertyKeyIdxToVectorPosMap.size(); i++) {
        assert(-1u == propertyKeyIdxToVectorPosMap[i]);
        auto& column = *nodePropertyColumns[i];
        auto nullValue = getNullValuePtrForDataType(column.dataType);
        // Here, we are assuming that the fileHandle of the column is not already in the
        // uncommittedPages map and hence, none of its pages are dirty. That is we assume that a
        // previous update or create node has not happened to the same set of nodes that are being
        // deleted.
        uncommittedPages.emplace(column.getFileHandle(), page_idx_to_dirty_page_map());
        auto& dirtyPagesMap = uncommittedPages[column.getFileHandle()];
        auto dataChunkIdx = 0u;
        while (dataChunkIdx < dataChunks.size()) {
            auto& dataChunk = *dataChunks[dataChunkIdx];
            auto nodeIDVector = dataChunk.valueVectors.back().get();
            updateNodePropertyColumn(0 /*currIdx*/, dataChunk.state->selectedSize, column,
                nullValue.get(), dirtyPagesMap, nullptr /*property value vector*/, nodeIDVector);
            dataChunkIdx++;
        }
    }
    dataChunks.clear();
}

// This function perform updates to specific pages of the column for `numUpdatesInDataChunk` number
// of tuples in the current dataChunk. For each nodeID in the `outValueVector`, its corresponding
// property value is read from the `propertyValueVector` and put in the appropriate page. If the
// `propertyValueVector` is a nullptr, `nullValue` is put in the page.
void LocalStorage::updateNodePropertyColumn(uint32_t currPosInDataChunk,
    uint64_t numUpdatesInDataChunk, Column& column, uint8_t* nullValue,
    page_idx_to_dirty_page_map& dirtyPagesMap, ValueVector* propertyValueVector,
    ValueVector* nodeIDVector) {
    for (auto i = currPosInDataChunk; i < numUpdatesInDataChunk + currPosInDataChunk; i++) {
        auto nodeOffset = ((nodeID_t*)nodeIDVector->values)[i].offset;
        auto pageCursor = column.getPageCursorForOffset(nodeOffset);
        auto page =
            putPageInDirtyPagesMap(dirtyPagesMap, pageCursor.idx, column.getFileHandle(), true);
        memcpy(page + pageCursor.offset,
            propertyValueVector ?
                propertyValueVector->values + currPosInDataChunk * column.elementSize :
                nullValue,
            column.elementSize);
    }
}

void LocalStorage::appendToNodePropertyColumn(uint32_t& dataChunkIdx, uint32_t& currPosInDataChunk,
    Column& column, uint8_t* nullValue, page_idx_to_dirty_page_map& dirtyPagesMap,
    uint32_t dataChunkSize, ValueVector* propertyValueVector, ValueVector* nodeIDVector) {
    auto nodeOffset = ((nodeID_t*)nodeIDVector->values)[currPosInDataChunk].offset;
    auto pageCursor = column.getPageCursorForOffset(nodeOffset);
    auto page = putPageInDirtyPagesMap(dirtyPagesMap, pageCursor.idx, column.getFileHandle(), true);
    auto freePosInPage = (PAGE_SIZE - pageCursor.offset) / column.elementSize;
    auto elementsLeftInDataChunk = dataChunkSize - currPosInDataChunk;
    auto numElementsToCopy = min(freePosInPage, (uint64_t)elementsLeftInDataChunk);
    if (propertyValueVector) {
        memcpy(page + pageCursor.offset,
            propertyValueVector->values + currPosInDataChunk * column.elementSize,
            numElementsToCopy * column.elementSize);
    } else {
        for (auto i = 0u; i < numElementsToCopy; i++) {
            memcpy(page + pageCursor.offset, nullValue, column.elementSize);
            pageCursor.offset += column.elementSize;
        }
    }
    if (freePosInPage >= elementsLeftInDataChunk) {
        dataChunkIdx++;
        currPosInDataChunk = 0;
    } else {
        currPosInDataChunk += freePosInPage;
    }
}

uint8_t* LocalStorage::putPageInDirtyPagesMap(page_idx_to_dirty_page_map& dirtyPagesMap,
    uint32_t pageIdx, FileHandle* fileHandle, bool createEmptyPage) {
    if (dirtyPagesMap.find(pageIdx) == dirtyPagesMap.end()) {
        auto newPage = new uint8_t[PAGE_SIZE];
        dirtyPagesMap.emplace(pageIdx, newPage);
        if (fileHandle->hasPage(pageIdx)) {
            fileHandle->readPage(newPage, pageIdx);
        } else if (!createEmptyPage) {
            throw invalid_argument("LocalStorage: cannot find page in file handle.");
        } else {
            if (uncommittedFileHandleNumPages.end() ==
                uncommittedFileHandleNumPages.find(fileHandle)) {
                uncommittedFileHandleNumPages[fileHandle] = pageIdx + 1;
            } else {
                uncommittedFileHandleNumPages[fileHandle] =
                    max(uncommittedFileHandleNumPages[fileHandle], (uint64_t)pageIdx + 1ul);
            }
        }
    }
    return dirtyPagesMap[pageIdx].get();
}

} // namespace transaction
} // namespace graphflow
