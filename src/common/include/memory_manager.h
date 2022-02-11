#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>

#include "src/common/include/configs.h"
#include "src/storage/include/buffer_manager.h"

using namespace std;
using namespace graphflow::storage;

namespace graphflow {
namespace common {

class MemoryManager;

// TODO(Deprecate). We should eventually remove this and rename BMBackedMemoryBlock to MemoryBlock.
struct OSBackedMemoryBlock {
public:
    explicit OSBackedMemoryBlock(uint64_t size) : size(size) {
        buffer = make_unique<uint8_t[]>(size);
        data = buffer.get();
    }

public:
    uint64_t size;
    uint8_t* data;

private:
    unique_ptr<uint8_t[]> buffer;
};

struct BMBackedMemoryBlock {

public:
    explicit BMBackedMemoryBlock(uint32_t pageIdx, uint8_t* data)
        : size(LARGE_PAGE_SIZE), pageIdx(pageIdx), data(data) {}

public:
    uint64_t size;
    uint32_t pageIdx;
    uint8_t* data;
};

// Memory manager for allocating/reclaiming large intermediate memory blocks. Currently it can
// allocate both memory directly from the OS and also through the buffer manager of the system. OS
// backed memory can be of any size but buffer manager backed memory is given in fixed size of
// LARGE_PAGE_SIZE, which is a constant defined in configs.h.
class MemoryManager {
public:
    explicit MemoryManager(BufferManager* bm) : bm(bm) {
        // Because the memory manager only manages blocks in memory, this file should never be
        // created, opened, or written to. It's a place holder name. We keep the name for logging
        // purposes.
        fh = make_shared<FileHandle>(
            "mm-place-holder-file-name", FileHandle::O_DiskBasedLargePagedTempFile);
    }

    // TODO(Deprecate). We should remove this eventually and only use allocateBMBackedBlock.
    unique_ptr<OSBackedMemoryBlock> allocateOSBackedBlock(
        uint64_t size, bool initializeToZero = false);

    unique_ptr<BMBackedMemoryBlock> allocateBMBackedBlock(bool initializeToZero = false);

    void freeBMBackedBlock(uint32_t pageIdx);

private:
    shared_ptr<FileHandle> fh;
    BufferManager* bm;
    stack<uint32_t> freePages;
    mutex memMgrLock;
};
} // namespace common
} // namespace graphflow
