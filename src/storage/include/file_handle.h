#pragma once

#include <atomic>
#include <memory>

#include "src/common/include/file_utils.h"

using namespace std;
using namespace graphflow::common;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

class BufferManager;

// FileHandle is the in-memory representation of the file. It holds an open file descriptor to the
// file as well as the state of each page in the file - if it is pinned in the Buffer Manager. File
// Handle is the bridge between a Column/Lists/Index and the Buffer Manager that abstracts the file
// in which that Column/Lists/Index is stored.
class FileHandle {
    friend class BufferManager;

public:
    explicit FileHandle(const string& path, int flags, bool isInMemory);
    ~FileHandle();

    bool inline hasPage(uint64_t pageIdx) const { return pageIdx < numPages; }

    void readPage(uint8_t* frame, uint64_t pageIdx) const;

    void writePage(uint8_t* buffer, uint64_t pageIdx) const;

private:
    inline uint64_t getFrameIdx(uint32_t pageIdx) { return pageIdxToFrameMap[pageIdx]->load(); }

    // Page lock management
    bool acquire(uint32_t pageIdx, bool block);
    void release(uint32_t pageIdx) { pageLocks[pageIdx]->clear(); }

    inline void swizzle(uint32_t pageIdx, uint64_t swizzledVal) {
        pageIdxToFrameMap[pageIdx]->store(swizzledVal);
    }

    inline void unswizzle(uint32_t pageIdx) { pageIdxToFrameMap[pageIdx]->store(UINT64_MAX); }

public:
    uint32_t numPages;

private:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<FileInfo> fileInfo;
    bool isInMemory;
    unique_ptr<atomic<uint64_t>>* pageIdxToFrameMap;
    unique_ptr<atomic_flag>* pageLocks;
    unique_ptr<uint8_t[]> buffer;
};

} // namespace storage
} // namespace graphflow
