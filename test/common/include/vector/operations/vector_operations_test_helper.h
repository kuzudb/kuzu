#pragma once

#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"

using namespace graphflow::common;
using ::testing::Test;

namespace graphflow {
namespace testing {

class TwoOperands {
public:
    virtual DataType getDataTypeOfOperands() = 0;
    virtual DataType getDataTypeOfResultVector() = 0;

    void initVectors() {
        memoryManager = make_unique<MemoryManager>();
        vector1 = make_shared<ValueVector>(memoryManager.get(), getDataTypeOfOperands());
        vector2 = make_shared<ValueVector>(memoryManager.get(), getDataTypeOfOperands());
        result = make_shared<ValueVector>(memoryManager.get(), getDataTypeOfResultVector());
    }

public:
    const int32_t NUM_TUPLES = 100;
    shared_ptr<ValueVector> vector1;
    shared_ptr<ValueVector> vector2;
    shared_ptr<ValueVector> result;
    unique_ptr<MemoryManager> memoryManager;
};

class OperandsInSameDataChunk : public TwoOperands {
public:
    void initDataChunk() {
        initVectors();
        dataChunk = make_shared<DataChunk>();
        dataChunk->state->selectedSize = NUM_TUPLES;
        dataChunk->append(vector1);
        dataChunk->append(vector2);
        dataChunk->append(result);
    }

public:
    shared_ptr<DataChunk> dataChunk;
};

class OperandsInDifferentDataChunks : public TwoOperands {

public:
    void initDataChunk() {
        initVectors();
        dataChunkWithVector1 = make_shared<DataChunk>();
        dataChunkWithVector1->state->selectedSize = NUM_TUPLES;
        dataChunkWithVector1->append(vector1);

        dataChunkWithVector2AndResult = make_shared<DataChunk>();
        dataChunkWithVector2AndResult->state->selectedSize = NUM_TUPLES;
        dataChunkWithVector2AndResult->append(vector2);
        dataChunkWithVector2AndResult->append(result);
    }

public:
    shared_ptr<DataChunk> dataChunkWithVector1;
    shared_ptr<DataChunk> dataChunkWithVector2AndResult;
};

} // namespace testing
} // namespace graphflow
