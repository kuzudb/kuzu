#include "storage/wal/wal.h"

#include "common/file_system/file_info.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/in_mem_file_writer.h"
#include "main/db_config.h"
#include "storage/storage_utils.h"
#include "storage/wal/local_wal.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

WAL::WAL(const std::string& dbPath, bool readOnly, VirtualFileSystem* vfs)
    : walPath{StorageUtils::getWALFilePath(dbPath)},
      inMemory{main::DBConfig::isDBPathInMemory(dbPath)}, readOnly{readOnly}, vfs{vfs} {}

WAL::~WAL() {}

void WAL::logCommittedWAL(LocalWAL& localWAL, main::ClientContext* context) {
    KU_ASSERT(!readOnly);
    if (inMemory || localWAL.getSize() == 0) {
        return; // No need to log empty WAL.
    }
    std::unique_lock lck{mtx};
    initWriter(context);
    localWAL.writer->flush(*writer);
    flushAndSyncNoLock();
}

void WAL::logAndFlushCheckpoint(main::ClientContext* context) {
    std::unique_lock lck{mtx};
    initWriter(context);
    CheckpointRecord walRecord;
    addNewWALRecordNoLock(walRecord);
    flushAndSyncNoLock();
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::clear() {
    std::unique_lock lck{mtx};
    writer->clear();
}

void WAL::reset() {
    std::unique_lock lck{mtx};
    fileInfo.reset();
    writer.reset();
    serializer.reset();
    vfs->removeFileIfExists(walPath);
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::flushAndSyncNoLock() {
    writer->flush();
    writer->sync();
}

uint64_t WAL::getFileSize() {
    std::unique_lock lck{mtx};
    return writer->getSize();
}

void WAL::initWriter(main::ClientContext* context) {
    if (writer) {
        return;
    }
    fileInfo = vfs->openFile(walPath,
        FileOpenFlags(FileFlags::CREATE_IF_NOT_EXISTS | FileFlags::READ_ONLY | FileFlags::WRITE),
        context);
    writer = std::make_shared<BufferedFileWriter>(*fileInfo);
    // WAL should always be APPEND only. We don't want to overwrite the file as it may still
    // contain records not replayed. This can happen if checkpoint is not triggered before the
    // Database is closed last time.
    writer->setFileOffset(fileInfo->getFileSize());
    serializer = std::make_unique<Serializer>(writer);
}

// NOLINTNEXTLINE(readability-make-member-function-const): semantically non-const function.
void WAL::addNewWALRecordNoLock(const WALRecord& walRecord) {
    KU_ASSERT(walRecord.type != WALRecordType::INVALID_RECORD);
    KU_ASSERT(!inMemory);
    walRecord.serialize(*serializer);
}

} // namespace storage
} // namespace kuzu
