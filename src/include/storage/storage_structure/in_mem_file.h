#pragma once

#include <atomic>

#include "common/constants.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/storage_structure/in_mem_page.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace common {
class VirtualFileSystem;
}

namespace storage {

// InMemFile holds a collection of in-memory page in the memory.
class InMemFile {
public:
    explicit InMemFile(
        std::shared_ptr<common::FileInfo> fileInfo, std::atomic<common::page_idx_t>& pageCounter);

    void addANewPage();

    inline InMemPage* getPage(common::page_idx_t pageIdx) const { return pages.at(pageIdx).get(); }

    // This function appends a string if the length of the string is larger than SHORT_STR_LENGTH,
    // otherwise, construct the ku_string for the rawString and return it.
    // Note that this function is not thread safe.
    void appendString(std::string_view rawString, common::ku_string_t& result);
    inline void appendString(const char* rawString, common::ku_string_t& result) {
        appendString(std::string_view(rawString), result);
    }

    std::string readString(common::ku_string_t* strInInMemOvfFile) const;
    bool equals(std::string_view keyToLookup, const common::ku_string_t& keyInEntry) const;

    void flush();

    inline common::page_idx_t getNextPageIndex(common::page_idx_t currentPageIndex) const {
        return *(common::page_idx_t*)(getPage(currentPageIndex)->data + END_OF_PAGE);
    }

private:
    static const uint64_t END_OF_PAGE =
        common::BufferPoolConstants::PAGE_4KB_SIZE - sizeof(common::page_idx_t);
    std::shared_ptr<common::FileInfo> fileInfo;
    std::unordered_map<common::page_idx_t, std::unique_ptr<InMemPage>> pages;
    PageCursor nextPosToAppend;
    std::atomic<common::page_idx_t>& pageCounter;
};

} // namespace storage
} // namespace kuzu
