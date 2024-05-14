#pragma once

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

    void flush();

    void resetOffsets() {
        fileOffset = 0;
        bufferOffset = 0;
    }

    uint64_t getFileSize() const;
    FileInfo& getFileInfo() { return *fileInfo; }

protected:
    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    std::unique_ptr<FileInfo> fileInfo;
    static const uint64_t BUFFER_SIZE = BufferPoolConstants::PAGE_4KB_SIZE;
};

class BufferedFileReader final : public Reader {
public:
    explicit BufferedFileReader(std::unique_ptr<FileInfo> fileInfo);

    void read(uint8_t* data, uint64_t size) override;

    bool finished() override;

private:
    static constexpr uint64_t BUFFER_SIZE = BufferPoolConstants::PAGE_4KB_SIZE;

    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    std::unique_ptr<FileInfo> fileInfo;
    uint64_t fileSize;
    uint64_t bufferSize;

private:
    void readNextPage();
};

} // namespace common
} // namespace kuzu
