#include "src/loader/include/in_mem_structure/column_utils.h"

namespace graphflow {
namespace loader {

void ColumnUtils::calculatePageCursor(
    const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor) {
    auto numElementsPerPage = PAGE_SIZE / numBytesPerElement;
    cursor.idx = nodeOffset / numElementsPerPage;
    cursor.offset = numBytesPerElement * (nodeOffset % numElementsPerPage);
}

} // namespace loader
} // namespace graphflow