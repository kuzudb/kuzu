#include "src/storage/include/file_handle.h"

#include <fcntl.h>
#include <unistd.h>

#include <limits>

namespace graphflow {
namespace storage {

FileHandle::FileHandle(string path, uint64_t numPages) {
    fileDescriptor = open(path.c_str(), O_RDONLY);
    if (-1 == fileDescriptor) {
        throw invalid_argument("Cannot open file: " + path);
    }
    for (auto i = 0ull; i < numPages; i++) {
        pageToFrameMap.push_back(UINT64_MAX);
    }
}

void FileHandle::readPage(char *frame, uint64_t pageIdx) {
    lseek(fileDescriptor, pageIdx * PAGE_SIZE, SEEK_SET);
    read(fileDescriptor, frame, PAGE_SIZE);
}

} // namespace storage
} // namespace graphflow
