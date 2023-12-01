#pragma once

#include <cstdint>
#include <memory>

#include "common/constants.h"
#include "common/serializer/reader.h"
#include "common/serializer/writer.h"

namespace kuzu {
namespace common {

struct FileInfo;

class BufferedFileWriter final : public Writer {
public:
    explicit BufferedFileWriter(std::unique_ptr<FileInfo> fileInfo);
    ~BufferedFileWriter() override;

    void write(const uint8_t* data, uint64_t size) override;

protected:
    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    std::unique_ptr<FileInfo> fileInfo;
    static const uint64_t BUFFER_SIZE = BufferPoolConstants::PAGE_4KB_SIZE;

    void flush();
};

class BufferedFileReader final : public Reader {
public:
    explicit BufferedFileReader(std::unique_ptr<FileInfo> fileInfo);

    void read(uint8_t* data, uint64_t size) override;

private:
    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    std::unique_ptr<FileInfo> fileInfo;
    static const uint64_t BUFFER_SIZE = BufferPoolConstants::PAGE_4KB_SIZE;

    void readNextPage();
};

} // namespace common
} // namespace kuzu
