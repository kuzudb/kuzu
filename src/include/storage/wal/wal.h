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
    WAL(std::string dbPath, bool readOnly, common::VirtualFileSystem* vfs);
    ~WAL();

    void logCommittedWAL(LocalWAL& localWAL, main::ClientContext* context);
    void logAndFlushCheckpoint(main::ClientContext* context);

    void clear();

    uint64_t getFileSize();

private:
    void initWriter(main::ClientContext* context);
    void addNewWALRecordNoLock(const WALRecord& walRecord);
    void flushAndSyncNoLock();

private:
    std::mutex mtx;
    std::string dbPath;
    bool readOnly;
    common::VirtualFileSystem* vfs;
    std::unique_ptr<common::FileInfo> fileInfo;
    std::shared_ptr<common::BufferedFileWriter> writer;
};

} // namespace storage
} // namespace kuzu
