#pragma once

#include "src/processor/include/physical_plan/result/factorized_table.h"

namespace graphflow {
namespace processor {

class FlatTupleIterator {
public:
    explicit FlatTupleIterator(
        FactorizedTable& factorizedTable, const vector<DataType>& columnDataTypes);

    inline bool hasNextFlatTuple() {
        return nextTupleIdx < factorizedTable.getNumTuples() || nextFlatTupleIdx < numFlatTuples;
    }

    shared_ptr<FlatTuple> getNextFlatTuple();

private:
    void readValueBufferToFlatTuple(uint64_t flatTupleValIdx, const uint8_t* valueBuffer);

    void readUnflatColToFlatTuple(uint64_t flatTupleValIdx, uint8_t* valueBuffer);

    void readFlatColToFlatTuple(uint32_t colIdx, uint8_t* valueBuffer);

    // The dataChunkPos may be not consecutive, which means some entries in the
    // flatTuplePositionsInDataChunk is invalid. We put pair(UINT64_MAX, UINT64_MAX) in the
    // invalid entries.
    inline bool isValidDataChunkPos(uint32_t dataChunkPos) const {
        return flatTuplePositionsInDataChunk[dataChunkPos].first != UINT64_MAX;
    }

    // We put pair(UINT64_MAX, UINT64_MAX) in all invalid entries in
    // FlatTuplePositionsInDataChunk.
    void updateInvalidEntriesInFlatTuplePositionsInDataChunk();

    // This function is used to update the number of elements in the dataChunk when we want
    // to iterate a new tuple.
    void updateNumElementsInDataChunk();

    // This function updates the flatTuplePositionsInDataChunk, so that getNextFlatTuple() can
    // correctly outputs the next flat tuple in the current tuple. For example, we want to read
    // two unflat columns, which are on different dataChunks A,B and both have 100 columns. The
    // flatTuplePositionsInDataChunk after the first call to getNextFlatTuple() looks like:
    // {dataChunkA : [0, 100]}, {dataChunkB : [0, 100]} This function updates the
    // flatTuplePositionsInDataChunk to: {dataChunkA: [1, 100]}, {dataChunkB: [0, 100]}. Meaning
    // that the getNextFlatTuple() should read the second element in the first unflat column and
    // the first element in the second unflat column.
    void updateFlatTuplePositionsInDataChunk();

    FactorizedTable& factorizedTable;
    uint8_t* currentTupleBuffer;
    uint64_t numFlatTuples;
    uint32_t nextFlatTupleIdx;
    uint64_t nextTupleIdx;
    // This field stores the (nextIdxToReadInDataChunk, numElementsInDataChunk) of each dataChunk.
    vector<pair<uint64_t, uint64_t>> flatTuplePositionsInDataChunk;

    vector<DataType> columnDataTypes;
    shared_ptr<FlatTuple> iteratorFlatTuple;
};

} // namespace processor
} // namespace graphflow
