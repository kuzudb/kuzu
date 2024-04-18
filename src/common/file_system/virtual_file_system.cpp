#include "common/file_system/virtual_file_system.h"

#include "common/assert.h"
#include "common/exception/io.h"
#include "common/file_system/local_file_system.h"
#include "main/client_context.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace kuzu {
namespace common {

VirtualFileSystem::VirtualFileSystem() {
    defaultFS = std::make_unique<LocalFileSystem>();
}

void VirtualFileSystem::registerFileSystem(std::unique_ptr<FileSystem> fileSystem) {
    subSystems.push_back(std::move(fileSystem));
}

std::unique_ptr<FileInfo> VirtualFileSystem::openFile(const std::string& path, int flags,
    main::ClientContext* context, FileLockType lockType) {
    return findFileSystem(path)->openFile(path, flags, context, lockType);
}

void VirtualFileSystem::recordLockInfo(const std::string & path, int flags, FileLockType lockType){
    std::unique_ptr<FileInfo> fileInfo;
    while(fileInfo == nullptr){
        try{
            fileInfo = openFile(path, flags, nullptr /* clientContext */, FileLockType::WRITE_LOCK);
        } catch (...){
            //open until we get the write lock
        }
    }
    int pid = getpid();
    auto fileSize = fileInfo->getFileSize();
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(&pid), sizeof(pid), fileSize /* offset */);
    fileInfo->writeFile(reinterpret_cast<const uint8_t*>(&lockType), sizeof(lockType),
        fileSize+sizeof(pid) /* offset */);
}

void VirtualFileSystem::checkLockInfoInSameProcess(std::string& path, int flags,
    FileLockType tryLockType) {
    std::unique_ptr<FileInfo> fileInfo;
    while(fileInfo == nullptr){
        try{
            fileInfo = openFile(path, flags, nullptr /* clientContext */, FileLockType::READ_LOCK);
        } catch (...){
            //open until we get the read lock? or we just hold no lock?
        }
    }
    auto fileSize = fileInfo->getFileSize();
    if (fileSize == 0) {
        return;
    }
    // try to find record, and check lockType
    uint64_t readSize = 0;
    while (readSize<fileSize) {
        int pid;
        fileInfo->readFromFile(reinterpret_cast<uint8_t*>(&pid), sizeof(pid), readSize);
        if (pid != getpid()) {
            readSize += sizeof(int) + sizeof(FileLockType);
            continue;
        }
        FileLockType lockType;
        fileInfo->readFromFile(reinterpret_cast<uint8_t*>(&lockType), sizeof(lockType), readSize+sizeof(pid));
        if(lockType==FileLockType::NO_LOCK) {
            readSize += sizeof(int) + sizeof(FileLockType);
            continue;
        }
        if (!(tryLockType == FileLockType::READ_LOCK && lockType == FileLockType::READ_LOCK)) {
            throw IOException("Could not set lock on file in the same process: " + path);
        }
    }
}

void VirtualFileSystem::deleteCurrentPidLockInfo(std::string& path, int flags){
    std::unique_ptr<FileInfo> fileInfo;
    while(fileInfo == nullptr){
        try{
            fileInfo = openFile(path, flags, nullptr /* clientContext */, FileLockType::WRITE_LOCK);
        } catch (...){
            //open until we get the write lock
        }
    }
    auto fileSize = fileInfo->getFileSize();
    if (fileSize == 0) {
        return;
    }
    // delete current pid
    std::unordered_map<int,FileLockType> activePids;
    uint64_t readSize = 0;
    while (readSize<fileSize){
        int pid;
        fileInfo->readFromFile(reinterpret_cast<uint8_t*>(&pid), sizeof(pid), readSize);
        if(pid==getpid()){
            readSize += sizeof(int) + sizeof(FileLockType);
            continue;
        }
        FileLockType lockType;
        fileInfo->readFromFile(reinterpret_cast<uint8_t*>(&lockType), sizeof(lockType), readSize+sizeof(pid));
        if(lockType!=FileLockType::NO_LOCK) {
            activePids.insert({pid,lockType});
        }
        readSize += sizeof(int) + sizeof(FileLockType);
    }
    fileInfo->truncate(0);
    readSize = 0;
    for (auto &pid : activePids) {
        fileInfo->writeFile(reinterpret_cast<const uint8_t*>(&pid.first), sizeof(pid.first), readSize);
        fileInfo->writeFile(reinterpret_cast<const uint8_t*>(&pid.second), sizeof(pid.second), readSize+sizeof(pid.first));
        readSize+=sizeof(pid.first)+sizeof(pid.second);
    }
}

std::vector<std::string> VirtualFileSystem::glob(main::ClientContext* context,
    const std::string& path) const {
    return findFileSystem(path)->glob(context, path);
}

void VirtualFileSystem::overwriteFile(const std::string& from, const std::string& to) const {
    findFileSystem(from)->overwriteFile(from, to);
}

void VirtualFileSystem::createDir(const std::string& dir) const {
    findFileSystem(dir)->createDir(dir);
}

void VirtualFileSystem::removeFileIfExists(const std::string& path) const {
    findFileSystem(path)->removeFileIfExists(path);
}

bool VirtualFileSystem::fileOrPathExists(const std::string& path) const {
    return findFileSystem(path)->fileOrPathExists(path);
}

std::string VirtualFileSystem::expandPath(main::ClientContext* context,
    const std::string& path) const {
    return findFileSystem(path)->expandPath(context, path);
}

void VirtualFileSystem::readFromFile(FileInfo& /*fileInfo*/, void* /*buffer*/,
    uint64_t /*numBytes*/, uint64_t /*position*/) const {
    KU_UNREACHABLE;
}

int64_t VirtualFileSystem::readFile(FileInfo& /*fileInfo*/, void* /*buf*/, size_t /*nbyte*/) const {
    KU_UNREACHABLE;
}

void VirtualFileSystem::writeFile(FileInfo& /*fileInfo*/, const uint8_t* /*buffer*/,
    uint64_t /*numBytes*/, uint64_t /*offset*/) const {
    KU_UNREACHABLE;
}

void VirtualFileSystem::syncFile(const FileInfo& fileInfo) const {
    findFileSystem(fileInfo.path)->syncFile(fileInfo);
}

int64_t VirtualFileSystem::seek(FileInfo& /*fileInfo*/, uint64_t /*offset*/, int /*whence*/) const {
    KU_UNREACHABLE;
}

void VirtualFileSystem::truncate(FileInfo& /*fileInfo*/, uint64_t /*size*/) const {
    KU_UNREACHABLE;
}

uint64_t VirtualFileSystem::getFileSize(const FileInfo& /*fileInfo*/) const {
    KU_UNREACHABLE;
}

FileSystem* VirtualFileSystem::findFileSystem(const std::string& path) const {
    for (auto& subSystem : subSystems) {
        if (subSystem->canHandleFile(path)) {
            return subSystem.get();
        }
    }
    return defaultFS.get();
}

} // namespace common
} // namespace kuzu
