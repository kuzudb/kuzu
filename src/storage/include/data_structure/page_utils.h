#pragma once

#include "src/common/include/configs.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

struct PageByteCursor {

    PageByteCursor(uint64_t idx, uint16_t offset) : idx{idx}, offset{offset} {};
    PageByteCursor() : PageByteCursor{-1ul, (uint16_t)-1} {};

    uint64_t idx;
    uint16_t offset;
};

struct PageElementCursor {

    PageElementCursor(uint64_t idx, uint16_t pos) : idx{idx}, pos{pos} {};
    PageElementCursor() : PageElementCursor{-1ul, (uint16_t)-1} {};

    uint64_t idx;
    uint16_t pos;
};

struct PageUtils {

    static uint8_t getNULLByteForOffset(const uint8_t* frame, uint16_t offset) {
        return frame[DEFAULT_PAGE_SIZE - 1 - (offset >> 3)];
    }

    static uint32_t getNumElementsInAPageWithNULLBytes(uint32_t elementSize) {
        auto numNULLBytes = (uint32_t)ceil((double)DEFAULT_PAGE_SIZE / ((elementSize << 3) + 1));
        return (DEFAULT_PAGE_SIZE - numNULLBytes) / elementSize;
    }

    static uint32_t getNumElementsInAPageWithoutNULLBytes(uint32_t elementSize) {
        return DEFAULT_PAGE_SIZE / elementSize;
    }

    // This function returns the page idx of the page where element will be found and the pos of the
    // element in the page as the offset.
    static PageElementCursor getPageElementCursorForOffset(
        const uint64_t& elementOffset, const uint32_t numElementsPerPage) {
        return PageElementCursor{
            elementOffset / numElementsPerPage, (uint16_t)(elementOffset % numElementsPerPage)};
    }
};

} // namespace storage
} // namespace graphflow