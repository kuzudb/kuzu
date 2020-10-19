#ifndef GRAPHFLOW_STORAGE_BUFFER_MANAGER_H_
#define GRAPHFLOW_STORAGE_BUFFER_MANAGER_H_

#include <memory>
#include <mutex>
#include <vector>

#include "src/common/include/storage_constants.h"
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
    FileHandle *fileHandle;
    uint32_t pageIdx;
    uint32_t pinCount;
    bool recentlyAccessed;
};

class BufferManager {

public:
    BufferManager(uint64_t maxSize);

    const char *pin(FileHandle &fileHandle, uint32_t pageIdx);
    void unpin(FileHandle &fileHandle, uint32_t pageIdx);

private:
    uint64_t evict();
    void readNewPageIntoFrame(Frame *frame, FileHandle &fileHandle, uint32_t pageIdx);
    
    inline bool isAFrame(uint64_t page) { return UINT64_MAX != page; }

private:
    vector<unique_ptr<Frame>> bufferPool;
    uint64_t clockHand;
    uint64_t maxPages;
    uint64_t usedPages;
    mutex bufferManagerMutex;
};

} // namespace storage
} // namespace graphflow

#endif // GRAPHFLOW_STORAGE_BUFFER_MANAGER_H_
