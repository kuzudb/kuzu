#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <memory>

using namespace std;

namespace graphflow {
namespace storage {

// Holds the reference to a location in a collection of pages of a Column or Lists.
typedef struct PageCursor {

    PageCursor(uint64_t idx, uint16_t offset) : idx{idx}, offset{offset} {};
    PageCursor() : PageCursor{-1ul, (uint16_t)-1} {};

    uint64_t idx;
    uint16_t offset;

} PageCursor;

// LogicalToPhysicalPageIDxMapper maps a logical pageIdx where a particular piece of data is located
// to the actual sequential index of the page in the file. If the data to be read is a small list,
// the logical pageIdx corresponds to the sequence of the page in a particular chunk while if the
// data is being read from a large list, the logical page idx is one of the pages which stores the
// complete large list. Note that pages of allocated to a chunk or a large list is not contiguously
// arragened in the lists file. For the case of reading from a column, we do not need a mapper since
// the logical page idx corresponds to the sequence in which pages are arranged in a column file.
class LogicalToPhysicalPageIdxMapper {

public:
    LogicalToPhysicalPageIdxMapper(const unique_ptr<uint32_t[]>& pageLists, uint32_t pageListHead)
        : pageLists{pageLists}, pageListHead{pageListHead} {}

    uint64_t getPageIdx(uint64_t pageIdx) {
        auto pageListPtr = pageListHead;
        while (3 <= pageIdx) {
            pageListPtr = pageLists[pageListPtr + 3];
            pageIdx -= 3;
        }
        return pageLists[pageListPtr + pageIdx];
    }

private:
    const unique_ptr<uint32_t[]>& pageLists;
    uint32_t pageListHead;
};

void saveListOfIntsToFile(const string& fName, unique_ptr<uint32_t[]>& data, uint32_t listSize);

uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName);

} // namespace storage
} // namespace graphflow
