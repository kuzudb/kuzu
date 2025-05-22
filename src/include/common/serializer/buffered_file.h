#pragma once

#include <memory>

#include "common/serializer/reader.h"
#include "common/serializer/writer.h"

namespace kuzu {
namespace common {

struct FileInfo;

class BufferedFileWriter final : public Writer {
public:
    explicit BufferedFileWriter(FileInfo& fileInfo);
    ~BufferedFileWriter() override;

    void write(const uint8_t* data, uint64_t size) override;

    void flush();
    void sync();

    // Note: this function resets the next file offset to be written. Make sure the buffer is empty.
    void setFileOffset(uint64_t fileOffset) { this->fileOffset = fileOffset; }
    uint64_t getFileOffset() const { return fileOffset; }
    void resetOffsets() {
        fileOffset = 0;
        bufferOffset = 0;
    }

    uint64_t getFileSize() const;
    FileInfo& getFileInfo() const { return fileInfo; }

protected:
    std::unique_ptr<uint8_t[]> buffer;
    uint64_t fileOffset, bufferOffset;
    FileInfo& fileInfo;
};

class BufferedFileReader final : public Reader {
public:
    explicit BufferedFileReader(std::unique_ptr<FileInfo> fileInfo);

    // Note: this function resets the next file offset to read.
    void resetReadOffset(uint64_t fileOffset) {
        this->fileOffset = fileOffset;
        bufferOffset = 0;
        bufferSize = 0;
    }

    void read(uint8_t* data, uint64_t size) override;

    bool finished() override;

private:
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
