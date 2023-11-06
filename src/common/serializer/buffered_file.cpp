#include "common/serializer/buffered_file.h"

#include "common/file_utils.h"

namespace kuzu {
namespace common {
void BufferedFileWriter::flush() {
    FileUtils::writeToFile(fileInfo.get(), buffer.get(), bufferOffset, fileOffset);
    fileOffset += bufferOffset;
    bufferOffset = 0;
    memset(buffer.get(), 0, BUFFER_SIZE);
}

void BufferedFileReader::readNextPage() {
    FileUtils::readFromFile(fileInfo.get(), buffer.get(), BUFFER_SIZE, fileOffset);
    fileOffset += BUFFER_SIZE;
    bufferOffset = 0;
}

} // namespace common
} // namespace kuzu
