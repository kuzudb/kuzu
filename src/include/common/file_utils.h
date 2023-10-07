#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

#include <filesystem>
#include <string>
#include <vector>

namespace kuzu {
namespace common {

enum class FileLockType : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

struct FileInfo {
#ifdef _WIN32
    FileInfo(std::string path, const void* handle) : path{std::move(path)}, handle{handle} {}
#else
    FileInfo(std::string path, const int fd) : path{std::move(path)}, fd{fd} {}
#endif

    ~FileInfo();

    int64_t getFileSize();

    const std::string path;
#ifdef _WIN32
    const void* handle;
#else
    const int fd;
#endif
};

class FileUtils {
public:
    static std::unique_ptr<FileInfo> openFile(
        const std::string& path, int flags, FileLockType lock_type = FileLockType::NO_LOCK);

    static void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position);
    static void writeToFile(
        FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes, uint64_t offset);
    // This function is a no-op if either file, from or to, does not exist.
    static void overwriteFile(const std::string& from, const std::string& to);
    static void copyFile(const std::string& from, const std::string& to,
        std::filesystem::copy_options options = std::filesystem::copy_options::none);
    static void createDir(const std::string& dir);
    static void createDirIfNotExists(const std::string& path);
    static void removeDir(const std::string& dir);

    static inline std::string joinPath(const std::string& base, const std::string& part) {
        return (std::filesystem::path(base) / part).string();
    }

    static void renameFileIfExists(const std::string& oldName, const std::string& newName);
    static void removeFileIfExists(const std::string& path);
    static inline void truncateFileToEmpty(FileInfo* fileInfo) {
        truncateFileToSize(fileInfo, 0 /* size */);
    }
    static void truncateFileToSize(FileInfo* fileInfo, uint64_t size);

    static inline bool fileOrPathExists(const std::string& path) {
        return std::filesystem::exists(path);
    }

    static std::vector<std::string> globFilePath(const std::string& path);

    static inline std::string getFileExtension(const std::filesystem::path& path) {
        return path.extension().string();
    }
};

} // namespace common
} // namespace kuzu
