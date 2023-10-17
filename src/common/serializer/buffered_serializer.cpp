#include "common/serializer/buffered_serializer.h"

#include <cstring>

namespace kuzu {
namespace common {

BufferedSerializer::BufferedSerializer(uint64_t maximumSize)
    : BufferedSerializer(std::make_unique<uint8_t[]>(maximumSize), maximumSize) {}

BufferedSerializer::BufferedSerializer(std::unique_ptr<uint8_t[]> data, uint64_t size)
    : maximumSize(size), data(data.get()) {
    blob.size = 0;
    blob.data = std::move(data);
}

void BufferedSerializer::write(const uint8_t* buffer, uint64_t len) {
    if (blob.size + len >= maximumSize) {
        do {
            maximumSize *= 2;
        } while (blob.size + len > maximumSize);
        auto new_data = std::make_unique<uint8_t[]>(maximumSize);
        memcpy(new_data.get(), data, blob.size);
        data = new_data.get();
        blob.data = std::move(new_data);
    }

    memcpy(data + blob.size, buffer, len);
    blob.size += len;
}

} // namespace common
} // namespace kuzu
