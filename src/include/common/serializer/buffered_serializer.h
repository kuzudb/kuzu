#pragma once

#include <memory>
#include <string>

#include "common/api.h"
#include "common/serializer/writer.h"

namespace kuzu {
namespace common {

// TODO(Ziyi): Move this to constants.h once we have a unified serializer design.
static constexpr uint64_t SERIALIZER_DEFAULT_SIZE = 1024;

struct BinaryData {
    std::unique_ptr<uint8_t[]> data;
    uint64_t size = 0;
};

class KUZU_API BufferedSerializer : public Writer {
public:
    // Serializes to a buffer allocated by the serializer, will expand when
    // writing past the initial threshold.
    explicit BufferedSerializer(uint64_t maximumSize = SERIALIZER_DEFAULT_SIZE);
    // Serializes to a provided (owned) data pointer.
    BufferedSerializer(std::unique_ptr<uint8_t[]> data, uint64_t size);

    // Retrieves the data after the writing has been completed.
    inline BinaryData getData() { return std::move(blob); }

    inline uint64_t getSize() const { return blob.size; }

    inline uint8_t* getBlobData() const { return blob.data.get(); }

    inline void reset() { blob.size = 0; }

    template<class T>
    void write(T element) {
        static_assert(std::is_trivially_destructible<T>(),
            "Write element must be trivially destructible");
        write(reinterpret_cast<const uint8_t*>(&element), sizeof(T));
    }

    void write(const uint8_t* buffer, uint64_t len) final;

    inline void writeBufferData(const std::string& str) {
        write(reinterpret_cast<const uint8_t*>(str.c_str()), str.size());
    }

    inline void writeBufferData(const char& ch) {
        write(reinterpret_cast<const uint8_t*>(&ch), sizeof(char));
    }

private:
    uint64_t maximumSize;
    uint8_t* data;

    BinaryData blob;
};

} // namespace common
} // namespace kuzu
