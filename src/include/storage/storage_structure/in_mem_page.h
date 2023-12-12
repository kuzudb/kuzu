#pragma once

#include <cstdint>
#include <memory>

namespace kuzu {
namespace storage {

class InMemPage {

public:
    explicit InMemPage();

    uint8_t* write(uint32_t byteOffsetInPage, const uint8_t* elem, uint32_t numBytesForElem) const;

public:
    uint8_t* data;

private:
    std::unique_ptr<uint8_t[]> buffer;
};

} // namespace storage
} // namespace kuzu
