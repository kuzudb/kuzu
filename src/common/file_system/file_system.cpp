#include "common/file_system/file_system.h"

namespace kuzu {
namespace common {

void FileSystem::overwriteFile(const std::string& /*from*/, const std::string& /*to*/) {
    KU_UNREACHABLE;
}

void FileSystem::copyFile(const std::string& /*from*/, const std::string& /*to*/) {
    KU_UNREACHABLE;
}

void FileSystem::createDir(const std::string& /*dir*/) const {
    KU_UNREACHABLE;
}

void FileSystem::removeFileIfExists(const std::string& /*path*/) {
    KU_UNREACHABLE;
}

bool FileSystem::fileOrPathExists(const std::string& /*path*/, main::ClientContext* /*context*/) {
    KU_UNREACHABLE;
}

std::string FileSystem::expandPath(main::ClientContext* /*context*/,
    const std::string& path) const {
    return path;
}

std::string FileSystem::joinPath(const std::string& base, const std::string& part) {
    return base + "/" + part;
}

std::string FileSystem::getFileExtension(const std::filesystem::path& path) {
    return path.extension().string();
}

std::string FileSystem::getFileName(const std::filesystem::path& path) {
    return path.filename().string();
}

void FileSystem::writeFile(FileInfo& /*fileInfo*/, const uint8_t* /*buffer*/, uint64_t /*numBytes*/,
    uint64_t /*offset*/) const {
    KU_UNREACHABLE;
}

void FileSystem::truncate(FileInfo& /*fileInfo*/, uint64_t /*size*/) const {
    KU_UNREACHABLE;
}

} // namespace common
} // namespace kuzu
