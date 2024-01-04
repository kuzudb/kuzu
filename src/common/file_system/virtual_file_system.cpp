#include "common/file_system/virtual_file_system.h"

#include "common/assert.h"
#include "common/file_system/local_file_system.h"

namespace kuzu {
namespace common {

VirtualFileSystem::VirtualFileSystem() {
    defaultFS = std::make_unique<LocalFileSystem>();
}

void VirtualFileSystem::registerFileSystem(std::unique_ptr<FileSystem> fileSystem) {
    subSystems.push_back(std::move(fileSystem));
}

FileSystem* VirtualFileSystem::findFileSystem(const std::string& path) {
    for (auto& subSystem : subSystems) {
        if (subSystem->canHandleFile(path)) {
            return subSystem.get();
        }
    }
    return defaultFS.get();
}

std::unique_ptr<FileInfo> VirtualFileSystem::openFile(
    const std::string& path, int flags, FileLockType lockType) {
    return findFileSystem(path)->openFile(path, flags, lockType);
}

std::vector<std::string> VirtualFileSystem::glob(const std::string& path) {
    return findFileSystem(path)->glob(path);
}

void VirtualFileSystem::overwriteFile(const std::string& from, const std::string& to) {
    findFileSystem(from)->overwriteFile(from, to);
}

void VirtualFileSystem::createDir(const std::string& dir) {
    findFileSystem(dir)->createDir(dir);
}

void VirtualFileSystem::removeFileIfExists(const std::string& path) {
    findFileSystem(path)->removeFileIfExists(path);
}

bool VirtualFileSystem::fileOrPathExists(const std::string& path) {
    return findFileSystem(path)->fileOrPathExists(path);
}

void VirtualFileSystem::readFromFile(
    FileInfo* /*fileInfo*/, void* /*buffer*/, uint64_t /*numBytes*/, uint64_t /*position*/) {
    KU_UNREACHABLE;
}

int64_t VirtualFileSystem::readFile(FileInfo* /*fileInfo*/, void* /*buf*/, size_t /*nbyte*/) {
    KU_UNREACHABLE;
}

void VirtualFileSystem::writeFile(
    FileInfo* /*fileInfo*/, const uint8_t* /*buffer*/, uint64_t /*numBytes*/, uint64_t /*offset*/) {
    KU_UNREACHABLE;
}

int64_t VirtualFileSystem::seek(FileInfo* /*fileInfo*/, uint64_t /*offset*/, int /*whence*/) {
    KU_UNREACHABLE;
}

void VirtualFileSystem::truncate(FileInfo* /*fileInfo*/, uint64_t /*size*/) {
    KU_UNREACHABLE;
}

uint64_t VirtualFileSystem::getFileSize(kuzu::common::FileInfo* /*fileInfo*/) {
    KU_UNREACHABLE;
}

} // namespace common
} // namespace kuzu
