#pragma once

#include "storage/wal/wal_record.h"

namespace kuzu {
namespace common {
class BufferedFileWriter;
class VirtualFileSystem;
} // namespace common

namespace storage {
class LocalWAL;
class WAL {
public:
    WAL(const std::string& dbPath, bool readOnly, common::VirtualFileSystem* vfs);
    ~WAL();

    void logCommittedWAL(LocalWAL& localWAL, main::ClientContext* context);
    void logAndFlushCheckpoint(main::ClientContext* context);

    // Clear any buffer in the WAL writer. Also truncate the WAL file to 0 bytes.
    void clear();
    // Reset the WAL writer to nullptr, and remove the WAL file if it exists.
    void reset();

    uint64_t getFileSize();

private:
    void initWriter(main::ClientContext* context);
    void addNewWALRecordNoLock(const WALRecord& walRecord);
    void flushAndSyncNoLock();

private:
    std::mutex mtx;
    std::string walPath;
    bool inMemory;
    bool readOnly;
    common::VirtualFileSystem* vfs;
    std::unique_ptr<common::FileInfo> fileInfo;
    std::shared_ptr<common::BufferedFileWriter> writer;
    std::unique_ptr<common::Serializer> serializer;
};

} // namespace storage
} // namespace kuzu
