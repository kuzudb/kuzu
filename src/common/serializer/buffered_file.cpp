#include "common/serializer/buffered_file.h"

#include <cstring>

#include "common/assert.h"
#include "common/exception/runtime.h"
#include "common/file_system/file_info.h"

namespace kuzu {
namespace common {

BufferedFileWriter::~BufferedFileWriter() {
    flush();
}

BufferedFileWriter::BufferedFileWriter(std::unique_ptr<FileInfo> fileInfo)
    : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)), fileOffset(0), bufferOffset(0),
      fileInfo(std::move(fileInfo)) {}

// TODO: write assumes size is always less than BUFFER_SIZE here. should change this when needed.
void BufferedFileWriter::write(const uint8_t* data, uint64_t size) {
    KU_ASSERT(size <= BUFFER_SIZE);
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
    fileInfo->writeFile(buffer.get(), bufferOffset, fileOffset);
    fileOffset += bufferOffset;
    bufferOffset = 0;
    memset(buffer.get(), 0, BUFFER_SIZE);
}

BufferedFileReader::BufferedFileReader(std::unique_ptr<FileInfo> fileInfo)
    : buffer(std::make_unique<uint8_t[]>(BUFFER_SIZE)), fileOffset(0), bufferOffset(0),
      fileInfo(std::move(fileInfo)), bufferSize{0} {
    fileSize = this->fileInfo->getFileSize();
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

bool BufferedFileReader::finished() {
    return bufferOffset >= bufferSize && fileSize <= fileOffset;
}

void BufferedFileReader::readNextPage() {
    bufferSize = std::min(fileSize - fileOffset, BUFFER_SIZE);
    if (bufferSize == 0) {
        throw RuntimeException(stringFormat("Reading past the end of the file {}", fileInfo->path));
    }
    fileInfo->readFromFile(buffer.get(), bufferSize, fileOffset);
    fileOffset += bufferSize;
    bufferOffset = 0;
}

} // namespace common
} // namespace kuzu
