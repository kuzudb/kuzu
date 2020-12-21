#include "src/storage/include/file_handle.h"

#include <fcntl.h>
#include <unistd.h>

#include <limits>

using namespace graphflow::common;

namespace graphflow {
namespace storage {

FileHandle::FileHandle(const string& path) {
    fileDescriptor = open(path.c_str(), O_RDONLY);
    if (-1 == fileDescriptor) {
        throw invalid_argument("Cannot open file: " + path);
    }
    auto fileLength = lseek(fileDescriptor, 0, SEEK_END);
    auto numPages = fileLength / PAGE_SIZE;
    if (0 != fileLength % PAGE_SIZE) {
        numPages++;
    }
    pageToFrameMap.resize(numPages);
    for (auto i = 0ull; i < numPages; i++) {
        pageToFrameMap[i] = UINT64_MAX;
    }
}

void FileHandle::readPage(char* frame, uint64_t pageIdx) {
    lseek(fileDescriptor, pageIdx * PAGE_SIZE, SEEK_SET);
    read(fileDescriptor, frame, PAGE_SIZE);
}

} // namespace storage
} // namespace graphflow
