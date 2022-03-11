#include "src/loader/include/in_mem_structure/column_utils.h"

namespace graphflow {
namespace loader {

PageElementCursor ColumnUtils::calcPageElementCursor(
    const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset) {
    return PageUtils::getPageElementCursorForOffset(
        nodeOffset, PageUtils::getNumElementsInAPageWithNULLBytes(numBytesPerElement));
}

uint64_t ColumnUtils::calcNumPagesInColumn(uint8_t numBytesPerElement, uint64_t maxElements) {
    auto numElementsPerPage = PageUtils::getNumElementsInAPageWithNULLBytes(numBytesPerElement);
    auto numPages = maxElements / numElementsPerPage;
    if (0 != maxElements % numElementsPerPage) {
        numPages++;
    }
    return numPages;
}

} // namespace loader
} // namespace graphflow