#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/storage/include/file_handle.h"

using namespace std;

namespace graphflow {
namespace storage {

class Frame;

class Frame {
    friend class BufferManager;

public:
    unique_ptr<char[]> buffer;

public:
    Frame();
    ~Frame();

private:
    FileHandle* fileHandle;
    uint32_t pageIdx;
    uint32_t pinCount;
    bool recentlyAccessed;
};

class BufferManager {

public:
    BufferManager(uint64_t maxSize);

    const char* pin(FileHandle& fileHandle, uint32_t pageIdx);
    void unpin(FileHandle& fileHandle, uint32_t pageIdx);
    unique_ptr<nlohmann::json> debugInfo();

private:
    uint64_t evict();
    void readNewPageIntoFrame(Frame* frame, FileHandle& fileHandle, uint32_t pageIdx);

    inline bool isAFrame(uint64_t page) { return UINT64_MAX != page; }

private:
    shared_ptr<spdlog::logger> logger;
    vector<unique_ptr<Frame>> bufferPool;
    uint64_t clockHand;
    uint64_t maxPages;
    uint64_t usedPages;
    mutex bufferManagerMutex;
};

} // namespace storage
} // namespace graphflow
