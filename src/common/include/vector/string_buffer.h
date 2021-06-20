#pragma once

#include <vector>

#include "src/common/include/gf_string.h"
#include "src/common/include/memory_manager.h"

namespace graphflow {
namespace common {

struct BufferBlock {
public:
    explicit BufferBlock(uint64_t size) : size{size}, currentOffset{0} {
        buffer = make_unique<uint8_t[]>(size);
        data = buffer.get();
    }

public:
    uint64_t size;
    uint64_t currentOffset;
    uint8_t* data;

private:
    unique_ptr<uint8_t[]> buffer;
};

class StringBuffer {
    static constexpr uint64_t MIN_BUFFER_BLOCK_SIZE = 4096;

public:
    explicit StringBuffer() : currentBlock{nullptr} {};

public:
    gf_string_t allocateLargeString(uint64_t len);

private:
    BufferBlock* currentBlock;
    vector<unique_ptr<BufferBlock>> blocks;
};
} // namespace common
} // namespace graphflow