#include "common/file_system/file_info.h"
#include "common/json_common.h"
#include "common/json_enums.h"
#include "main/client_context.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace json_extension {

struct JsonScanBufferHandle {
    uint64_t bufferIdx;
    std::unique_ptr<storage::MemoryBuffer> buffer;
    uint64_t bufferSize;

    JsonScanBufferHandle(uint64_t bufferIdx, std::unique_ptr<storage::MemoryBuffer> buffer,
        uint64_t bufferSize)
        : bufferIdx{bufferIdx}, buffer{std::move(buffer)}, bufferSize{bufferSize} {}
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

    uint64_t getBufferIdx() { return bufferIdx++; }

    void throwParseError(yyjson_read_err& err, const std::string& extra = "") const;

    JsonFileHandle* getFileHandle() const { return fileHandle.get(); }

    std::string getFileName() const { return fileName; }

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
