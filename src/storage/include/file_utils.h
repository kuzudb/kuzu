#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <string>

using namespace std;

namespace graphflow {
namespace storage {

class FileUtils {
public:
    static int openFile(const string& path, int flags);
    static void closeFile(int fd);
    static inline int64_t getFileSize(int fd);

    static void readFromFile(int fd, void* buffer, int64_t numBytes, uint64_t position);
    static void writeToFile(int fd, void* buffer, int64_t numBytes, uint64_t offset);

    static inline string joinPath(const string& base, const string& part) {
        return filesystem::path(base) / part;
    }
    static inline bool fileExists(const string& path) { return filesystem::exists(path); }
};
} // namespace storage
} // namespace graphflow
