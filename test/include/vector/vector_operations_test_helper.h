#pragma once

#include "common/data_chunk/data_chunk.h"
#include "gtest/gtest.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using ::testing::Test;

namespace kuzu {
namespace testing {

class TwoOperands {
public:
    virtual DataTypeID getDataTypeOfOperands() = 0;
    virtual DataTypeID getDataTypeOfResultVector() = 0;

    void initVectors() {
        bufferManager =
            std::make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = std::make_unique<MemoryManager>(bufferManager.get());
        vector1 = std::make_shared<ValueVector>(getDataTypeOfOperands(), memoryManager.get());
        vector2 = std::make_shared<ValueVector>(getDataTypeOfOperands(), memoryManager.get());
        result = std::make_shared<ValueVector>(getDataTypeOfResultVector(), memoryManager.get());
    }

public:
    const int32_t NUM_TUPLES = 100;
    std::unique_ptr<BufferManager> bufferManager;
    std::unique_ptr<MemoryManager> memoryManager;
    std::shared_ptr<ValueVector> vector1;
    std::shared_ptr<ValueVector> vector2;
    std::shared_ptr<ValueVector> result;
};

class OperandsInSameDataChunk : public TwoOperands {
public:
    void initDataChunk() {
        initVectors();
        dataChunk = std::make_shared<DataChunk>(3);
        dataChunk->state->selVector->selectedSize = NUM_TUPLES;
        dataChunk->insert(0, vector1);
        dataChunk->insert(1, vector2);
        dataChunk->insert(2, result);
    }

public:
    std::shared_ptr<DataChunk> dataChunk;
};

class OperandsInDifferentDataChunks : public TwoOperands {

public:
    void initDataChunk() {
        initVectors();
        dataChunkWithVector1 = std::make_shared<DataChunk>(1);
        dataChunkWithVector1->state->selVector->selectedSize = NUM_TUPLES;
        dataChunkWithVector1->insert(0, vector1);

        dataChunkWithVector2AndResult = std::make_shared<DataChunk>(2);
        dataChunkWithVector2AndResult->state->selVector->selectedSize = NUM_TUPLES;
        dataChunkWithVector2AndResult->insert(0, vector2);
        dataChunkWithVector2AndResult->insert(1, result);
    }

public:
    std::shared_ptr<DataChunk> dataChunkWithVector1;
    std::shared_ptr<DataChunk> dataChunkWithVector2AndResult;
};

} // namespace testing
} // namespace kuzu
