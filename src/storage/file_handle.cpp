#include "src/storage/include/file_handle.h"

#include "src/common/include/configs.h"
#include "src/common/include/file_utils.h"
#include "src/common/include/utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

FileHandle::FileHandle(const string& path, int flags, bool isInMemory)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")},
      fileDescriptor{FileUtils::openFile(path, flags)}, isInMemory{isInMemory} {
    logger->trace("FileHandle: Path {}", path);
    auto fileLength = FileUtils::getFileSize(fileDescriptor);
    numPages = fileLength >> PAGE_SIZE_LOG_2;
    if (0 != (fileLength & (PAGE_SIZE - 1))) {
        numPages++;
    }
    if (isInMemory) {
        logger->trace("FileHandle[in-memory]: Size {}B", fileLength);
        buffer = FileUtils::readFile(fileDescriptor);
    } else {
        logger->trace("FileHandle[disk]: Size {}B, #4KB-pages {}", fileLength, numPages);
        pageIdxToFrameMap = new unique_ptr<atomic<uint64_t>>[numPages];
        pageLocks = new unique_ptr<atomic_flag>[numPages];
        for (auto i = 0ull; i < numPages; i++) {
            pageLocks[i] = make_unique<atomic_flag>();
            pageIdxToFrameMap[i] = make_unique<atomic<uint64_t>>(UINT64_MAX);
        }
    }
}

FileHandle::~FileHandle() {
    FileUtils::closeFile(fileDescriptor);
    if (!isInMemory) {
        delete[] pageLocks;
        delete[] pageIdxToFrameMap;
    }
}

bool FileHandle::acquire(uint32_t pageIdx, bool block) {
    if (block) {
        while (pageLocks[pageIdx]->test_and_set(memory_order_acquire)) { // spinning
        }
        return true;
    }
    return !pageLocks[pageIdx]->test_and_set(memory_order_acquire);
}

void FileHandle::readPage(uint8_t* frame, uint64_t pageIdx) const {
    FileUtils::readFromFile(fileDescriptor, frame, PAGE_SIZE, pageIdx << PAGE_SIZE_LOG_2);
}

void FileHandle::writePage(uint8_t* buffer, uint64_t pageIdx) const {
    FileUtils::writeToFile(fileDescriptor, buffer, PAGE_SIZE, pageIdx << PAGE_SIZE_LOG_2);
}

} // namespace storage
} // namespace graphflow
