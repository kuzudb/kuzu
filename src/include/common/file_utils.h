#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <string>

using namespace std;

namespace kuzu {
namespace common {

struct FileInfo {
    FileInfo(string path, const int fd) : path{std::move(path)}, fd{fd} {}

    ~FileInfo() {
        if (fd != -1) {
            close(fd);
        }
    }

    const string path;
    const int fd;
};

class FileUtils {
public:
    static unique_ptr<FileInfo> openFile(const string& path, int flags);
    static void closeFile(int fd);

    static void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position);
    static void writeToFile(
        FileInfo* fileInfo, uint8_t* buffer, uint64_t numBytes, uint64_t offset);
    // This function is a no-op if either file, from or to, does not exist.
    static void overwriteFile(const string& from, const string& to);
    static void createDir(const string& dir);
    static void removeDir(const string& dir);

    static inline string joinPath(const string& base, const string& part) {
        return filesystem::path(base) / part;
    }

    static void renameFileIfExists(const string& oldName, const string& newName);
    static void removeFileIfExists(const string& path);
    static void truncateFileToEmpty(FileInfo* fileInfo);
    static inline bool fileOrPathExists(const string& path) { return filesystem::exists(path); }

    static inline int64_t getFileSize(int fd) {
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return -1;
        }
        return s.st_size;
    }
};
} // namespace common
} // namespace kuzu
