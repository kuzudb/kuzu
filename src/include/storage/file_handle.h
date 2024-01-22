#pragma once

#include <memory>
#include <shared_mutex>

#include "common/assert.h"
#include "common/constants.h"
#include "common/file_system/file_info.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {
class VirtualFileSystem;
}

namespace storage {
// FileHandle holds basic state information of a file, including FileInfo, flags, pageSize,
// numPages, and pageCapacity. Also, FileHandle provides utility methods to read/write pages from/to
// the file.
class FileHandle {
public:
    constexpr static uint8_t isLargePagedMask{0b0000'0001}; // represents 1st least sig. bit (LSB)
    constexpr static uint8_t isNewInMemoryTmpFileMask{0b0000'0010}; // represents 2nd LSB
    // createIfNotExistsMask only applies to existing db files; tmp i-memory files are not created
    constexpr static uint8_t createIfNotExistsMask{0b0000'0100}; // represents 3rd LSB
    constexpr static uint8_t isReadOnlyMask{0b0000'1000};        // represents 4th LSB

    // READ_ONLY subsumes DEFAULT_PAGED, PERSISTENT, and NO_CREATE.
    constexpr static uint8_t O_PERSISTENT_FILE_READ_ONLY{0b0000'1000};
    constexpr static uint8_t O_PERSISTENT_FILE_NO_CREATE{0b0000'0000};
    constexpr static uint8_t O_PERSISTENT_FILE_CREATE_NOT_EXISTS{0b0000'0100};
    constexpr static uint8_t O_IN_MEM_TEMP_FILE{0b0000'0011};

    FileHandle(const std::string& path, uint8_t flags, common::VirtualFileSystem* vfs);
    virtual ~FileHandle() = default;

    common::page_idx_t addNewPage();
    common::page_idx_t addNewPages(common::page_idx_t numPages);

    inline void readPage(uint8_t* frame, common::page_idx_t pageIdx) const {
        KU_ASSERT(pageIdx < numPages);
        fileInfo->readFromFile(frame, getPageSize(), pageIdx * getPageSize());
    }
    inline void writePage(uint8_t* buffer, common::page_idx_t pageIdx) const {
        KU_ASSERT(pageIdx < numPages);
        fileInfo->writeFile(buffer, getPageSize(), pageIdx * getPageSize());
    }

    inline bool isLargePaged() const { return flags & isLargePagedMask; }
    inline bool isNewTmpFile() const { return flags & isNewInMemoryTmpFileMask; }
    inline bool isReadOnlyFile() const { return flags & isReadOnlyMask; }
    inline bool createFileIfNotExists() const { return flags & createIfNotExistsMask; }

    inline common::page_idx_t getNumPages() const { return numPages; }
    inline common::FileInfo* getFileInfo() const { return fileInfo.get(); }
    inline uint64_t getPageSize() const {
        return isLargePaged() ? common::BufferPoolConstants::PAGE_256KB_SIZE :
                                common::BufferPoolConstants::PAGE_4KB_SIZE;
    }

protected:
    virtual common::page_idx_t addNewPageWithoutLock();
    void constructExistingFileHandle(const std::string& path, common::VirtualFileSystem* vfs);
    void constructNewFileHandle(const std::string& path);

protected:
    uint8_t flags;
    std::unique_ptr<common::FileInfo> fileInfo;
    // Actually allocated/used number of pages in the file.
    uint32_t numPages;
    // This is the maximum number of pages the filehandle can currently support.
    uint32_t pageCapacity;
    // Intended to be used to coordinate calls to functions that change in the internal data
    // structures of the file handle.
    std::shared_mutex fhSharedMutex;
};

} // namespace storage
} // namespace kuzu
