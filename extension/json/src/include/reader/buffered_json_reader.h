#pragma once

#include "common/file_system/file_info.h"
#include "common/json_enums.h"
#include "main/client_context.h"
#include "processor/operator/persistent/reader/file_error_handler.h"
#include "storage/buffer_manager/memory_manager.h"
#include "yyjson.h"

namespace kuzu {
namespace json_extension {

struct JsonScanBufferHandle {
    uint64_t bufferIdx;
    std::unique_ptr<storage::MemoryBuffer> buffer;
    uint64_t bufferSize;
    std::atomic<uint64_t> numReaders;

    JsonScanBufferHandle(uint64_t bufferIdx, std::unique_ptr<storage::MemoryBuffer> buffer,
        uint64_t bufferSize, uint64_t numReaders)
        : bufferIdx{bufferIdx}, buffer{std::move(buffer)}, bufferSize{bufferSize},
          numReaders{numReaders} {}
};

struct JsonFileHandle {
    std::unique_ptr<common::FileInfo> fileInfo;
    uint64_t filesSize;
    uint64_t readPosition;
    std::atomic<uint64_t> requestedReads;
    std::atomic<uint64_t> actualReads;
    std::atomic<bool> lastReadRequested;

    explicit JsonFileHandle(std::unique_ptr<common::FileInfo> fileInfo)
        : fileInfo{std::move(fileInfo)}, filesSize{this->fileInfo->getFileSize()}, readPosition{0},
          requestedReads{0}, actualReads{0}, lastReadRequested{false} {}

    bool isLastReadRequested() const { return lastReadRequested; }

    common::FileInfo* getFileInfo() const { return fileInfo.get(); }

    bool getPositionAndSize(uint64_t& position, uint64_t& size, uint64_t requestedSize);

    void readAtPosition(uint8_t* pointer, uint64_t size, uint64_t position, bool& finishFile);
};

struct BufferedJSONReaderOptions {
    JsonScanFormat format;

    explicit BufferedJSONReaderOptions(JsonScanFormat format) : format{format} {}
};

class BufferedJsonReader {
public:
    BufferedJsonReader(main::ClientContext& context, std::string fileName,
        BufferedJSONReaderOptions options);

    JsonScanFormat getFormat() const { return options.format; }

    void setFormat(JsonScanFormat format) { options.format = format; }

    void insertBuffer(uint64_t bufferIdx, std::unique_ptr<JsonScanBufferHandle>&& buffer) {
        std::lock_guard<std::mutex> mtx(lock);
        bufferMap.insert(make_pair(bufferIdx, std::move(buffer)));
    }

    std::string reconstructLine(uint64_t startPosition, uint64_t endPosition);

    std::unique_ptr<storage::MemoryBuffer> removeBuffer(const JsonScanBufferHandle& handle);

    uint64_t getBufferIdx() { return bufferIdx++; }

    void throwParseError(const yyjson_read_err& err, bool completedParsingObject,
        const processor::WarningSourceData& errorData,
        processor::LocalFileErrorHandler* errorHandler, const std::string& extra = "") const;

    JsonFileHandle* getFileHandle() const { return fileHandle.get(); }

    std::string getFileName() const { return fileName; }

    JsonScanBufferHandle* getBuffer(common::idx_t bufferIdx) {
        std::lock_guard<std::mutex> guard(lock);
        return bufferMap.contains(bufferIdx) ? bufferMap.at(bufferIdx).get() : nullptr;
    }

public:
    std::mutex lock;

private:
    main::ClientContext& context;
    std::unique_ptr<JsonFileHandle> fileHandle;
    std::string fileName;
    BufferedJSONReaderOptions options;
    common::idx_t bufferIdx;
    std::unordered_map<uint64_t, std::unique_ptr<JsonScanBufferHandle>> bufferMap;
};

} // namespace json_extension
} // namespace kuzu
