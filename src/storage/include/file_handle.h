#pragma once

#include <memory>
#include <vector>

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"

using namespace std;

namespace graphflow {
namespace storage {

class BufferManager;

// FileHandle is the in-memory representation of the file. It holds an open file descriptor to the
// file as well as the state of each page in the file - if it is pinned in the Buffer Manager. File
// Handle is the bridge between a Column/Lists and the Buffer Manager that abstracts the file in
// which that Column/Lists is stored.
class FileHandle {
    friend class BufferManager;

public:
    FileHandle(const string& path);
    ~FileHandle();

    bool hasPage(uint8_t* frame, uint64_t pageIdx) { return pageIdx < numPages; }

    void readPage(uint8_t* frame, uint64_t pageIdx);

private:
    inline uint64_t getFrameIdx(uint32_t pageIdx) { return pageIdxToFrameMap[pageIdx]->load(); }

    // Page lock management
    bool acquire(uint32_t pageIdx, bool block);
    void release(uint32_t pageIdx) { pageLocks[pageIdx]->clear(); }

    inline void swizzle(uint32_t pageIdx, uint64_t swizzledVal) {
        pageIdxToFrameMap[pageIdx]->store(swizzledVal);
    }

    inline void unswizzle(uint32_t pageIdx) { pageIdxToFrameMap[pageIdx]->store(UINT64_MAX); }

private:
    shared_ptr<spdlog::logger> logger;
    const int fileDescriptor;
    uint32_t numPages;
    unique_ptr<atomic<uint64_t>>* pageIdxToFrameMap;
    unique_ptr<atomic_flag>* pageLocks;
};

} // namespace storage
} // namespace graphflow
