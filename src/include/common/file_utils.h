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

static std::mutex mtx;

struct FileInfo {
    FileInfo(std::string path, const int fd) : path{std::move(path)}, fd{fd} {}

    ~FileInfo() {
        if (fd != -1) {
            close(fd);
        }
    }

    const std::string path;
    const int fd;
};

class FileUtils {
public:
    static std::unique_ptr<FileInfo> openFile(const std::string& path, int flags);

    static void createFileWithSize(const std::string& path, uint64_t size);
    static void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position);
    static void writeToFile(
        FileInfo* fileInfo, uint8_t* buffer, uint64_t numBytes, uint64_t offset);
    // This function is a no-op if either file, from or to, does not exist.
    static void overwriteFile(const std::string& from, const std::string& to);
    static void createDir(const std::string& dir);
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

    static inline int64_t getFileSize(int fd) {
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return -1;
        }
        return s.st_size;
    }

    static std::vector<std::string> globFilePath(const std::string& path);

    static std::vector<std::string> findAllDirectories(const std::string& path);

    static inline std::string getParentPath(const std::filesystem::path& path) {
        return path.parent_path().string();
    }

    static inline std::string getParentPathStem(const std::filesystem::path& path) {
        return path.parent_path().stem().string();
    }

    static inline std::string getFileExtension(const std::filesystem::path& path) {
        return path.extension().string();
    }
};

} // namespace common
} // namespace kuzu
