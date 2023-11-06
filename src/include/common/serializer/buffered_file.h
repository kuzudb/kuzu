#pragma once

#include <cstdint>
#include <cstring>
#include <memory>

#include "common/serializer/reader.h"
#include "common/serializer/writer.h"

namespace kuzu {
namespace common {

struct FileInfo;

class BufferedFileWriter : public Writer {
public:
    ~BufferedFileWriter() { flush(); }
    explicit BufferedFileWriter(std::unique_ptr<FileInfo> fileInfo)
        : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)), fileOffset(0), bufferOffset(0),
          fileInfo(std::move(fileInfo)) {}

    inline void write(const uint8_t* data, uint64_t size) final {
        if (bufferOffset + size <= BUFFER_SIZE) {
            memcpy(&buffer[bufferOffset], data, size);
            bufferOffset += size;
        } else {
            auto toCopy = BUFFER_SIZE - bufferOffset;
            memcpy(&buffer[bufferOffset], data, toCopy);
            bufferOffset += toCopy;
            flush();
            auto remaining = size - toCopy;
            memcpy(buffer.get(), data + toCopy, remaining);
            bufferOffset += remaining;
        }
    }

protected:
    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    std::unique_ptr<FileInfo> fileInfo;
    static const uint64_t BUFFER_SIZE = 4096;

    void flush();
};

class BufferedFileReader : public Reader {
public:
    explicit BufferedFileReader(std::unique_ptr<FileInfo> fileInfo)
        : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)), fileOffset(0), bufferOffset(0),
          fileInfo(std::move(fileInfo)) {
        readNextPage();
    }

    inline void read(uint8_t* data, uint64_t size) final {
        if (bufferOffset + size <= BUFFER_SIZE) {
            memcpy(data, &buffer[bufferOffset], size);
            bufferOffset += size;
        } else {
            auto toCopy = BUFFER_SIZE - bufferOffset;
            memcpy(data, &buffer[bufferOffset], toCopy);
            bufferOffset += toCopy;
            readNextPage();
            auto remaining = size - toCopy;
            memcpy(data + toCopy, buffer.get(), remaining);
            bufferOffset += remaining;
        }
    }

private:
    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    std::unique_ptr<FileInfo> fileInfo;
    static const uint64_t BUFFER_SIZE = 4096;

    void readNextPage();
};

} // namespace common
} // namespace kuzu
