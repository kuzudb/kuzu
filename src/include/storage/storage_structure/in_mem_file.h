#pragma once

#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/storage_structure/in_mem_page.h"

namespace kuzu {
namespace storage {

// InMemFile holds a collection of in-memory page in the memory.
class InMemFile {
public:
    explicit InMemFile(std::string filePath);

    uint32_t addANewPage(bool setToZero = false);

    inline InMemPage* getPage(common::page_idx_t pageIdx) const { return pages[pageIdx].get(); }

    // This function appends a string if the length of the string is larger than SHORT_STR_LENGTH,
    // otherwise, construct the ku_string for the rawString and return it.
    // Note that this function is not thread safe.
    common::ku_string_t appendString(const char* rawString);

    std::string readString(common::ku_string_t* strInInMemOvfFile);

    void flush();

private:
    std::string filePath;
    std::vector<std::unique_ptr<InMemPage>> pages;
    common::page_idx_t nextPageIdxToAppend;
    common::page_offset_t nextOffsetInPageToAppend;
};

} // namespace storage
} // namespace kuzu
