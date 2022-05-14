#pragma once

#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/wal/wal_record.h"

namespace graphflow {
namespace storage {

class StorageManager;

class WALReplayer {
public:
    WALReplayer(StorageManager& storageManager, BufferManager& bufferManager, bool isCheckpoint);

    void replay();

private:
    void replayWALRecord(WALRecord& walRecord);

private:
    StorageManager& storageManager;
    BufferManager& bufferManager;
    shared_ptr<FileHandle> walFileHandle;
    unique_ptr<uint8_t[]> pageBuffer;
    bool isCheckpoint; // if true does redo operations; if false does undo operations
};

} // namespace storage
} // namespace graphflow
