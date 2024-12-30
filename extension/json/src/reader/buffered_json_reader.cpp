#include "reader/buffered_json_reader.h"

#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"

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
    : context{context}, fileName{std::move(fileName)}, options{options}, bufferIdx{0} {
    auto fileInfo = context.getVFSUnsafe()->openFile(this->fileName, FileFlags::READ_ONLY);
    fileHandle = std::make_unique<JsonFileHandle>(std::move(fileInfo));
}

std::string BufferedJsonReader::reconstructLine(uint64_t startPosition, uint64_t endPosition) {
    KU_ASSERT(endPosition >= startPosition);

    std::lock_guard<std::mutex> lockGuard{lock};

    std::string ret;
    ret.resize(endPosition - startPosition);

    bool finishedFile = false;
    fileHandle->readAtPosition(reinterpret_cast<uint8_t*>(ret.data()), ret.size(), startPosition,
        finishedFile);

    return StringUtils::ltrimNewlines(StringUtils::rtrimNewlines(ret));
}

std::unique_ptr<storage::MemoryBuffer> BufferedJsonReader::removeBuffer(
    const JsonScanBufferHandle& handle) {
    std::lock_guard<std::mutex> guard(lock);
    KU_ASSERT(bufferMap.contains(handle.bufferIdx));
    auto result = std::move(bufferMap.at(handle.bufferIdx)->buffer);
    bufferMap.erase(handle.bufferIdx);
    return result;
}

void BufferedJsonReader::throwParseError(const yyjson_read_err& err, bool completedParsingObject,
    const processor::WarningSourceData& errorData, processor::LocalFileErrorHandler* errorHandler,
    const std::string& extra) const {
    errorHandler->handleError(
        processor::CopyFromFileError{common::stringFormat("Malformed JSON: {}. {}", err.msg, extra),
            errorData, completedParsingObject});
}

} // namespace json_extension
} // namespace kuzu
