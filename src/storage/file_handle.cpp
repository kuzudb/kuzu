#include "src/storage/include/file_handle.h"

#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <limits>

using namespace graphflow::common;

namespace graphflow {
namespace storage {

FileHandle::FileHandle(const string& path)
    : logger{spdlog::get("storage")}, fileDescriptor(open(path.c_str(), O_RDONLY)) {
    logger->trace("FileHandle: Path {}", path);
    if (-1 == fileDescriptor) {
        throw invalid_argument("Cannot open file: " + path);
    }
    auto fileLength = lseek(fileDescriptor, 0, SEEK_END);
    auto numPages = fileLength / PAGE_SIZE;
    if (0 != fileLength % PAGE_SIZE) {
        numPages++;
    }
    logger->trace("FileHandle: Size {}B, #4KB-pages {}", fileLength, numPages);
    pageIdxToFrameMap = new unique_ptr<atomic<uint64_t>>[numPages];
    pageLocks = new unique_ptr<atomic_flag>[numPages];
    for (auto i = 0ull; i < numPages; i++) {
        pageLocks[i] = make_unique<atomic_flag>();
        pageIdxToFrameMap[i] = make_unique<atomic<uint64_t>>(UINT64_MAX);
    }
}

FileHandle::~FileHandle() {
    delete[] pageLocks;
    delete[] pageIdxToFrameMap;
}

bool FileHandle::acquire(uint32_t pageIdx, bool block) {
    if (block) {
        while (pageLocks[pageIdx]->test_and_set(memory_order_acquire)) { // spinning
        }
        return true;
    }
    return !pageLocks[pageIdx]->test_and_set(memory_order_acquire);
}

void FileHandle::readPage(uint8_t* frame, uint64_t pageIdx) {
    pread(fileDescriptor, frame, PAGE_SIZE, pageIdx * PAGE_SIZE);
}

} // namespace storage
} // namespace graphflow
