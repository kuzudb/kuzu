#pragma once

#include "common/serializer/reader.h"

namespace kuzu {
namespace common {

struct BufferReader : Reader {
    BufferReader(uint8_t* data, size_t dataSize) : data(data), dataSize(dataSize), readSize(0) {}

    void read(uint8_t* outputData, uint64_t size) final {
        memcpy(outputData, data + readSize, size);
        readSize += size;
    }

    bool finished() final { return (readSize >= dataSize); }

    uint8_t* data;
    size_t dataSize;
    size_t readSize;
};

} // namespace common
} // namespace kuzu
