#include "common/file_system/virtual_file_system.h"

#include "common/assert.h"
#include "common/file_system/local_file_system.h"
#include "main/client_context.h"

namespace kuzu {
namespace common {

VirtualFileSystem::VirtualFileSystem() {
    defaultFS = std::make_unique<LocalFileSystem>();
}

void VirtualFileSystem::registerFileSystem(std::unique_ptr<FileSystem> fileSystem) {
    subSystems.push_back(std::move(fileSystem));
}

std::unique_ptr<FileInfo> VirtualFileSystem::openFile(
    const std::string& path, int flags, main::ClientContext* context, FileLockType lockType) {
    return findFileSystem(path)->openFile(path, flags, context, lockType);
}

std::vector<std::string> VirtualFileSystem::glob(
    main::ClientContext* context, const std::string& path) const {
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

std::string VirtualFileSystem::expandPath(
    main::ClientContext* context, const std::string& path) const {
    return findFileSystem(path)->expandPath(context, path);
}

void VirtualFileSystem::readFromFile(
    FileInfo* /*fileInfo*/, void* /*buffer*/, uint64_t /*numBytes*/, uint64_t /*position*/) const {
    KU_UNREACHABLE;
}

int64_t VirtualFileSystem::readFile(FileInfo* /*fileInfo*/, void* /*buf*/, size_t /*nbyte*/) const {
    KU_UNREACHABLE;
}

void VirtualFileSystem::writeFile(FileInfo* /*fileInfo*/, const uint8_t* /*buffer*/,
    uint64_t /*numBytes*/, uint64_t /*offset*/) const {
    KU_UNREACHABLE;
}

int64_t VirtualFileSystem::seek(FileInfo* /*fileInfo*/, uint64_t /*offset*/, int /*whence*/) const {
    KU_UNREACHABLE;
}

void VirtualFileSystem::truncate(FileInfo* /*fileInfo*/, uint64_t /*size*/) const {
    KU_UNREACHABLE;
}

uint64_t VirtualFileSystem::getFileSize(kuzu::common::FileInfo* /*fileInfo*/) const {
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
