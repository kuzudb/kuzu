#pragma once

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// PageHandle stores the state a pageIdx when reading from a list or a column across multiple calls
// to the readValues() function. The pageIdx is the pageIdx of the page that is currently pinned by
// the Column/Lists to make the Frame available to the readValues() caller.
class PageHandle {

public:
    PageHandle() : pageIdx{-1u} {};

    inline bool hasPageIdx() const { return -1u != pageIdx; }
    inline uint32_t getPageIdx() const { return pageIdx; }
    inline void setPageIdx(uint32_t pageIdx) { this->pageIdx = pageIdx; }
    inline void resetPageIdx() { pageIdx = -1u; }

private:
    uint32_t pageIdx;
};

} // namespace storage
} // namespace graphflow
