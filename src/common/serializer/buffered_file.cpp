#include "common/serializer/buffered_file.h"

#include <cstring>

#include "common/file_utils.h"

namespace kuzu {
namespace common {

BufferedFileWriter::~BufferedFileWriter() {
    flush();
}

BufferedFileWriter::BufferedFileWriter(std::unique_ptr<FileInfo> fileInfo)
    : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)), fileOffset(0), bufferOffset(0),
      fileInfo(std::move(fileInfo)) {}

void BufferedFileWriter::write(const uint8_t* data, uint64_t size) {
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

void BufferedFileWriter::flush() {
    FileUtils::writeToFile(fileInfo.get(), buffer.get(), bufferOffset, fileOffset);
    fileOffset += bufferOffset;
    bufferOffset = 0;
    memset(buffer.get(), 0, BUFFER_SIZE);
}

BufferedFileReader::BufferedFileReader(std::unique_ptr<FileInfo> fileInfo)
    : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)), fileOffset(0), bufferOffset(0),
      fileInfo(std::move(fileInfo)) {
    readNextPage();
}

void BufferedFileReader::read(uint8_t* data, uint64_t size) {
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

void BufferedFileReader::readNextPage() {
    FileUtils::readFromFile(fileInfo.get(), buffer.get(), BUFFER_SIZE, fileOffset);
    fileOffset += BUFFER_SIZE;
    bufferOffset = 0;
}

} // namespace common
} // namespace kuzu
