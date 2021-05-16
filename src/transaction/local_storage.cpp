#include "src/transaction/include/local_storage.h"

namespace graphflow {
namespace transaction {

void LocalStorage::computeCreateNode(vector<uint32_t> propertyKeyIdxToVectorPosMap,
    label_t nodeLabel, vector<BaseColumn*> nodePropertyColumns, uint64_t numNodes) {
    auto numRecycledIDs = assignNodeIDs(nodeLabel);
    uncommittedLabelToNumNodes.emplace(nodeLabel, numNodes + max(0ul, numTuples - numRecycledIDs));
    for (auto i = 0u; i < propertyKeyIdxToVectorPosMap.size(); i++) {
        auto numUpdates = numRecycledIDs;
        auto& column = *nodePropertyColumns[i];
        uncommittedPages.emplace(column.getFileHandle(), pageIdxToDirtyPageMap());
        auto& dirtyPagesMap = uncommittedPages[column.getFileHandle()];
        auto nullValue = make_unique<uint8_t[]>(getDataTypeSize(column.dataType));
        switch (column.dataType) {
        case INT32:
            memcpy(nullValue.get(), &NULL_INT32, getDataTypeSize(column.dataType));
            break;
        case DOUBLE:
            memcpy(nullValue.get(), &NULL_DOUBLE, getDataTypeSize(column.dataType));
            break;
        case BOOL:
            memcpy(nullValue.get(), &NULL_BOOL, getDataTypeSize(column.dataType));
            break;
        case STRING: {
            gf_string_t nullStr;
            nullStr.set(string(&gf_string_t::EMPTY_STRING));
            memcpy(nullValue.get(), &NULL_BOOL, getDataTypeSize(column.dataType));
            break;
        }
        default:
            assert(false);
        }
        ValueVector* propertyValueVector = nullptr;
        auto dataChunkIdx = 0u;
        auto idxInDataChunk = 0u;
        while (dataChunkIdx < dataChunks.size()) {
            auto& dataChunk = *dataChunks[dataChunkIdx];
            auto nodeIDVector = (NodeIDVector*)dataChunk.valueVectors.back().get();
            if (-1 != propertyKeyIdxToVectorPosMap[i]) {
                propertyValueVector = dataChunk.valueVectors[propertyKeyIdxToVectorPosMap[i]].get();
            }
            if (0 < numUpdates) {
                updateNodePropertyColumn(dataChunkIdx, idxInDataChunk,
                    propertyKeyIdxToVectorPosMap[i], numUpdates, column, nullValue.get(),
                    dirtyPagesMap, dataChunk.state->size, propertyValueVector, nodeIDVector);
            }
            // If there are more created nodes that are left in the datachunk.
            if (0 == numUpdates) {
                appendToNodePropertyColumn(dataChunkIdx, idxInDataChunk,
                    propertyKeyIdxToVectorPosMap[i], column, nullValue.get(), dirtyPagesMap,
                    dataChunk.state->size, propertyValueVector, nodeIDVector);
            }
        }
    }
    dataChunks.clear();
}

uint32_t LocalStorage::assignNodeIDs(label_t nodeLabel) {
    // TODO: get the list of nodeIDs from the free list, and recycle them.
    // // Since we do not have the free list implemented yet, assume that the number of recycled
    // // nodeIds is 0.
    // numRecycledIds gives the number of tuples in the collection of dataChunks that
    // will be inserted to the required columns in the order they appear. Once dealt with these
    // tuples, rest of the tuples' data is insereted sequentially in appropriate columns.
    auto numRecycledIds = 0u;
    for (auto& dataChunk : dataChunks) {
        // The last vector in the dataChunk is the nodeIDVector containing newly assigned nodeIDs.
        auto nodeIDVector = make_shared<NodeIDVector>(nodeLabel, NodeIDCompressionScheme(), false);
        dataChunk->append(nodeIDVector);
        // TODO: Insert property keys in the hash index.
        // We first use the recycled nodeIDs and then assign from after `numNodes`, sequentially.
        // while (there is more nodeIDs to recycles, recycle) {
        //  }
        // handle the remaining.
    }
    return numRecycledIds;
}

void LocalStorage::updateNodePropertyColumn(uint32_t& dataChunkIdx, uint32_t& idxInDataChunk,
    uint32_t vectorPos, uint32_t& numUpdates, BaseColumn& column, uint8_t* nullValue,
    pageIdxToDirtyPageMap& dirtyPagesMap, uint32_t dataChunkSize, ValueVector* propertyValueVector,
    NodeIDVector* nodeIDVector) {
    auto numUpdatesInDataChunk = numUpdates >= dataChunkSize ? dataChunkSize : numUpdates;
    numUpdates -= numUpdatesInDataChunk;
    nodeID_t nodeID;
    for (auto i = idxInDataChunk; i < numUpdatesInDataChunk + idxInDataChunk; i++) {
        nodeIDVector->readNodeOffset(i, nodeID);
        auto pageCursor = column.getPageCursorForOffset(nodeID.offset);
        if (dirtyPagesMap.find(pageCursor.idx) == dirtyPagesMap.end()) {
            auto newPage = new uint8_t[PAGE_SIZE];
            dirtyPagesMap.emplace(pageCursor.idx, newPage);
            column.getFileHandle()->readPage(newPage, pageCursor.idx);
        }
        auto& page = dirtyPagesMap[pageCursor.idx];
        if (propertyValueVector) {
            memcpy(page.get() + pageCursor.offset,
                propertyValueVector + dataChunkIdx * column.elementSize, column.elementSize);
        } else {
            memcpy(page.get() + pageCursor.offset, nullValue, column.elementSize);
        }
    }
    if (numUpdatesInDataChunk + idxInDataChunk == dataChunkSize) {
        // dataChunk exhausted, continue to the next dataChunk
        dataChunkIdx++;
        idxInDataChunk = 0;
    } else {
        idxInDataChunk += numUpdatesInDataChunk;
    }
}

void LocalStorage::appendToNodePropertyColumn(uint32_t& dataChunkIdx, uint32_t& idxInDataChunk,
    uint32_t vectorPos, BaseColumn& column, uint8_t* nullValue,
    pageIdxToDirtyPageMap& dirtyPagesMap, uint32_t dataChunkSize, ValueVector* propertyValueVector,
    NodeIDVector* nodeIDVector) {
    nodeID_t nodeID;
    nodeIDVector->readNodeOffset(idxInDataChunk, nodeID);
    auto pageCursor = column.getPageCursorForOffset(nodeID.offset);
    if (dirtyPagesMap.find(pageCursor.idx) == dirtyPagesMap.end()) {
        auto newPage = new uint8_t[PAGE_SIZE];
        dirtyPagesMap.emplace(pageCursor.idx, newPage);
        if (column.getFileHandle()->hasPage(newPage, pageCursor.idx)) {
            column.getFileHandle()->readPage(newPage, pageCursor.idx);
        }
    }
    auto& page = dirtyPagesMap[pageCursor.idx];
    auto freePosInPage = (PAGE_SIZE - pageCursor.offset) / column.elementSize;
    auto elementsLeftInDataChunk = dataChunkSize - idxInDataChunk;
    auto numElementsToCopy = min(freePosInPage, (uint64_t)elementsLeftInDataChunk);
    if (propertyValueVector) {
        memcpy(page.get() + pageCursor.offset,
            propertyValueVector + dataChunkIdx * column.elementSize,
            numElementsToCopy * column.elementSize);
    } else {
        for (auto i = 0u; i < numElementsToCopy; i++) {
            memcpy(page.get() + pageCursor.offset, nullValue, column.elementSize);
            pageCursor.offset += column.elementSize;
        }
    }
    if (freePosInPage >= elementsLeftInDataChunk) {
        dataChunkIdx++;
        idxInDataChunk = 0;
    } else {
        idxInDataChunk += freePosInPage;
    }
}

} // namespace transaction
} // namespace graphflow