#pragma once

#include <memory>
#include <vector>

#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"

using namespace std;

namespace graphflow {
namespace storage {

class BufferManager;

class FileHandle {
    friend class BufferManager;

public:
    FileHandle(const string& path);

private:
    inline uint64_t getPageIdxEntry(uint32_t pageIdx) { return pageToFrameMap[pageIdx]; }
    inline void swizzle(uint32_t pageIdx, uint64_t swizzledVal) {
        pageToFrameMap[pageIdx] = swizzledVal;
    }
    inline void unswizzle(uint32_t pageIdx) { pageToFrameMap[pageIdx] = UINT64_MAX; }

    void readPage(uint8_t* frame, uint64_t pageIdx);

private:
    shared_ptr<spdlog::logger> logger;
    int fileDescriptor;
    vector<uint64_t> pageToFrameMap;
};

} // namespace storage
} // namespace graphflow
