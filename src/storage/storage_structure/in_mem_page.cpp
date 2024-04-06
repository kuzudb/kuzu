#include "storage/storage_structure/in_mem_page.h"

#include <cstring>

#include "common/constants.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemPage::InMemPage() {
    buffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
    data = buffer.get();
}

uint8_t* InMemPage::write(uint32_t byteOffsetInPage, const uint8_t* elem,
    uint32_t numBytesForElem) const {
    memcpy(data + byteOffsetInPage, elem, numBytesForElem);
    return data + byteOffsetInPage;
}

} // namespace storage
} // namespace kuzu
