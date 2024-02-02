#pragma once

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
    explicit InMemFile(std::string filePath, common::VirtualFileSystem* vfs);

    uint32_t addANewPage(bool setToZero = false);

    inline InMemPage* getPage(common::page_idx_t pageIdx) const { return pages[pageIdx].get(); }

    // This function appends a string if the length of the string is larger than SHORT_STR_LENGTH,
    // otherwise, construct the ku_string for the rawString and return it.
    // Note that this function is not thread safe.
    common::ku_string_t appendString(std::string_view rawString);
    inline common::ku_string_t appendString(const char* rawString) {
        return appendString(std::string_view(rawString));
    }

    std::string readString(common::ku_string_t* strInInMemOvfFile);

    void flush();

private:
    std::string filePath;
    std::unique_ptr<common::FileInfo> fileInfo;
    std::vector<std::unique_ptr<InMemPage>> pages;
    PageCursor nextPosToAppend;
};

} // namespace storage
} // namespace kuzu
