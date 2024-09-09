#include "reader/buffered_json_reader.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"

namespace kuzu {
namespace json_extension {

using namespace common;

JsonScanBufferHandle::JsonScanBufferHandle(uint64_t bufferIdx, JsonScanBuffer&& buffer,
    uint64_t bufferSize)
    : bufferIdx{bufferIdx}, buffer{std::move(buffer)}, bufferSize{bufferSize} {}

JsonFileHandle::JsonFileHandle(std::unique_ptr<common::FileInfo> fileInfo)
    : fileInfo{std::move(fileInfo)}, filesSize{this->fileInfo->getFileSize()}, readPosition(0),
      requestedReads{0}, actualReads{0}, lastReadRequested{false} {}

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
    : context{context}, fileName{std::move(fileName)}, options{std::move(options)}, thrown{false} {
    auto fileInfo = context.getVFSUnsafe()->openFile(this->fileName, FileFlags::READ_ONLY);
    fileHandle = std::make_unique<JsonFileHandle>(std::move(fileInfo));
}

void BufferedJsonReader::throwParseError(yyjson_read_err& err, const std::string& extra) const {
    auto unit = options.format == JsonScanFormat::NEWLINE_DELIMITED ? "line" : "record/value";
    auto line = 0;
    // TODO(Ziyi): report error line number.
    throw common::RuntimeException{
        common::stringFormat("Malformed JSON in file \"{}\", at byte {} in {} {}: {}. {}", fileName,
            err.pos + 1, unit, line + 1, err.msg, extra)};
}

} // namespace json_extension
} // namespace kuzu
