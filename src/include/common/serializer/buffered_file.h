#pragma once

#include <algorithm>
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
        uint64_t remaining = size;
        do {
            uint64_t copyBytes = std::min(remaining, BUFFER_SIZE - bufferOffset);
            memcpy(&buffer[bufferOffset], data, copyBytes);
            data += copyBytes;
            remaining -= copyBytes;
            bufferOffset += copyBytes;
            if (bufferOffset == BUFFER_SIZE) {
                flush();
            }
        } while (remaining > 0);
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
        uint64_t remaining = size;
        do {
            uint64_t copyBytes = std::min(remaining, BUFFER_SIZE - bufferOffset);
            memcpy(data, &buffer[bufferOffset], copyBytes);
            data += copyBytes;
            remaining -= copyBytes;
            bufferOffset += copyBytes;
            if (bufferOffset == BUFFER_SIZE) {
                readNextPage();
            }
        } while (remaining > 0);
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
