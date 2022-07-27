#include "src/processor/include/physical_plan/result/factorized_tuple_iterator.h"

namespace graphflow {
namespace processor {

FlatTupleIterator::FlatTupleIterator(
    FactorizedTable& factorizedTable, const vector<DataType>& columnDataTypes)
    : factorizedTable{factorizedTable}, numFlatTuples{0}, nextFlatTupleIdx{0}, nextTupleIdx{1},
      columnDataTypes{columnDataTypes} {
    if (factorizedTable.getNumTuples()) {
        currentTupleBuffer = factorizedTable.getTuple(0);
        numFlatTuples = factorizedTable.getNumFlatTuples(0);
        updateNumElementsInDataChunk();
        updateInvalidEntriesInFlatTuplePositionsInDataChunk();
    }
    // Note: there is difference between column data types and value types in iteratorFlatTuple.
    // Column data type could be UNSTRUCTURED but value type must be a structured type.
    iteratorFlatTuple = make_shared<FlatTuple>(columnDataTypes);
    assert(columnDataTypes.size() == factorizedTable.tableSchema->getNumColumns());
}

shared_ptr<FlatTuple> FlatTupleIterator::getNextFlatTuple() {
    // Go to the next tuple if we have iterated all the flat tuples of the current tuple.
    if (nextFlatTupleIdx >= numFlatTuples) {
        currentTupleBuffer = factorizedTable.getTuple(nextTupleIdx);
        numFlatTuples = factorizedTable.getNumFlatTuples(nextTupleIdx);
        nextFlatTupleIdx = 0;
        updateNumElementsInDataChunk();
        nextTupleIdx++;
    }
    for (auto i = 0ul; i < factorizedTable.getTableSchema()->getNumColumns(); i++) {
        auto column = factorizedTable.getTableSchema()->getColumn(i);
        if (column->isFlat()) {
            readFlatColToFlatTuple(i, currentTupleBuffer);
        } else {
            readUnflatColToFlatTuple(i, currentTupleBuffer);
        }
    }
    updateFlatTuplePositionsInDataChunk();
    nextFlatTupleIdx++;
    return iteratorFlatTuple;
}

void FlatTupleIterator::readValueBufferToFlatTuple(
    uint64_t flatTupleValIdx, const uint8_t* valueBuffer) {
    switch (columnDataTypes[flatTupleValIdx].typeID) {
    case INT64: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.int64Val = *((int64_t*)valueBuffer);
    } break;
    case BOOL: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.booleanVal = *((bool*)valueBuffer);
    } break;
    case DOUBLE: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.doubleVal = *((double_t*)valueBuffer);
    } break;
    case STRING: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.strVal = *((gf_string_t*)valueBuffer);
    } break;
    case NODE_ID: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.nodeID = *((nodeID_t*)valueBuffer);
    } break;
    case UNSTRUCTURED: {
        *iteratorFlatTuple->getValue(flatTupleValIdx) = *((Value*)valueBuffer);
    } break;
    case DATE: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.dateVal = *((date_t*)valueBuffer);
    } break;
    case TIMESTAMP: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.timestampVal =
            *((timestamp_t*)valueBuffer);
    } break;
    case INTERVAL: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.intervalVal = *((interval_t*)valueBuffer);
    } break;
    case LIST: {
        iteratorFlatTuple->getValue(flatTupleValIdx)->val.listVal = *((gf_list_t*)valueBuffer);
    } break;
    default:
        assert(false);
    }
}

void FlatTupleIterator::readUnflatColToFlatTuple(uint64_t colIdx, uint8_t* valueBuffer) {
    auto overflowValue =
        (overflow_value_t*)(valueBuffer + factorizedTable.getTableSchema()->getColOffset(colIdx));
    auto columnInFactorizedTable = factorizedTable.getTableSchema()->getColumn(colIdx);
    auto tupleSizeInOverflowBuffer = Types::getDataTypeSize(columnDataTypes[colIdx]);
    valueBuffer =
        overflowValue->value +
        tupleSizeInOverflowBuffer *
            flatTuplePositionsInDataChunk[columnInFactorizedTable->getDataChunkPos()].first;
    iteratorFlatTuple->nullMask[colIdx] = factorizedTable.isOverflowColNull(
        overflowValue->value + tupleSizeInOverflowBuffer * overflowValue->numElements,
        flatTuplePositionsInDataChunk[columnInFactorizedTable->getDataChunkPos()].first, colIdx);
    if (!iteratorFlatTuple->nullMask[colIdx]) {
        readValueBufferToFlatTuple(colIdx, valueBuffer);
    }
}

void FlatTupleIterator::readFlatColToFlatTuple(uint32_t colIdx, uint8_t* valueBuffer) {
    iteratorFlatTuple->nullMask[colIdx] = factorizedTable.isNonOverflowColNull(
        valueBuffer + factorizedTable.getTableSchema()->getNullMapOffset(), colIdx);
    if (!iteratorFlatTuple->nullMask[colIdx]) {
        readValueBufferToFlatTuple(
            colIdx, valueBuffer + factorizedTable.getTableSchema()->getColOffset(colIdx));
    }
}

void FlatTupleIterator::updateInvalidEntriesInFlatTuplePositionsInDataChunk() {
    for (auto i = 0u; i < flatTuplePositionsInDataChunk.size(); i++) {
        bool isValidEntry = false;
        for (auto j = 0u; j < factorizedTable.getTableSchema()->getNumColumns(); j++) {
            if (factorizedTable.getTableSchema()->getColumn(j)->getDataChunkPos() == i) {
                isValidEntry = true;
                break;
            }
        }
        if (!isValidEntry) {
            flatTuplePositionsInDataChunk[i] = make_pair(UINT64_MAX, UINT64_MAX);
        }
    }
}

void FlatTupleIterator::updateNumElementsInDataChunk() {
    auto colOffsetInTupleBuffer = 0ul;
    for (auto i = 0u; i < factorizedTable.getTableSchema()->getNumColumns(); i++) {
        auto column = factorizedTable.getTableSchema()->getColumn(i);
        // If this is an unflat column, the number of elements is stored in the
        // overflow_value_t struct. Otherwise, the number of elements is 1.
        auto numElementsInDataChunk =
            column->isFlat() ?
                1 :
                ((overflow_value_t*)(currentTupleBuffer + colOffsetInTupleBuffer))->numElements;
        if (column->getDataChunkPos() >= flatTuplePositionsInDataChunk.size()) {
            flatTuplePositionsInDataChunk.resize(column->getDataChunkPos() + 1);
        }
        flatTuplePositionsInDataChunk[column->getDataChunkPos()] =
            make_pair(0 /* nextIdxToReadInDataChunk */, numElementsInDataChunk);
        colOffsetInTupleBuffer += column->getNumBytes();
    }
}

void FlatTupleIterator::updateFlatTuplePositionsInDataChunk() {
    for (auto i = 0u; i < flatTuplePositionsInDataChunk.size(); i++) {
        if (!isValidDataChunkPos(i)) {
            continue;
        }
        flatTuplePositionsInDataChunk.at(i).first++;
        // If we have output all elements in the current column, we reset the
        // nextIdxToReadInDataChunk in the current column to 0.
        if (flatTuplePositionsInDataChunk.at(i).first >=
            flatTuplePositionsInDataChunk.at(i).second) {
            flatTuplePositionsInDataChunk.at(i).first = 0;
        } else {
            // If the current dataChunk is not full, then we don't need to update the next
            // dataChunk.
            break;
        }
    }
}

} // namespace processor
} // namespace graphflow