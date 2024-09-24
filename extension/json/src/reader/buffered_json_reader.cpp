#include "reader/buffered_json_reader.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"

namespace kuzu {
namespace json_extension {

using namespace common;

bool JsonFileHandle::getPositionAndSize(uint64_t& position, uint64_t& size,
    uint64_t requestedSize) {
    KU_ASSERT(requestedSize != 0);
    if (lastReadRequested) {
        return false;
    }
    position = readPosition;
    size = std::min<uint64_t>(requestedSize, filesSize - readPosition);
    readPosition += size;
    requestedReads++;
    if (size == 0) {
        lastReadRequested = true;
    }
    return true;
}

void JsonFileHandle::readAtPosition(uint8_t* pointer, uint64_t size, uint64_t position,
    bool& finishFile) {
    fileInfo->readFromFile(pointer, size, position);
    auto incrementedActualReads = ++actualReads;
    if (lastReadRequested && incrementedActualReads == requestedReads) {
        finishFile = true;
    }
}

BufferedJsonReader::BufferedJsonReader(main::ClientContext& context, std::string fileName,
    BufferedJSONReaderOptions options)
    : context{context}, fileName{std::move(fileName)}, options{std::move(options)}, bufferIdx{0} {
    auto fileInfo = context.getVFSUnsafe()->openFile(this->fileName, FileFlags::READ_ONLY);
    fileHandle = std::make_unique<JsonFileHandle>(std::move(fileInfo));
}

std::unique_ptr<storage::MemoryBuffer> BufferedJsonReader::removeBuffer(
    JsonScanBufferHandle& handle) {
    std::lock_guard<std::mutex> guard(lock);
    KU_ASSERT(bufferMap.contains(handle.bufferIdx));
    auto result = std::move(bufferMap.at(handle.bufferIdx)->buffer);
    bufferMap.erase(handle.bufferIdx);
    return result;
}

void BufferedJsonReader::throwParseError(yyjson_read_err& err, const std::string& extra) const {
    auto unit = options.format == JsonScanFormat::NEWLINE_DELIMITED ? "line" : "record/value";
    // TODO(Ziyi): report error line number.
    throw common::RuntimeException{
        common::stringFormat("Malformed JSON in file \"{}\", at byte {} in {}: {}. {}", fileName,
            err.pos + 1, unit, err.msg, extra)};
}

} // namespace json_extension
} // namespace kuzu
