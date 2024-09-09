#include "common/file_system/file_info.h"
#include "common/json_common.h"
#include "common/json_enums.h"
#include "main/client_context.h"

namespace kuzu {
namespace json_extension {

struct JsonScanBuffer {
    std::unique_ptr<uint8_t[]> data;

    JsonScanBuffer() : data{std::make_unique<uint8_t[]>(JsonConstant::SCAN_BUFFER_CAPACITY)} {}

    uint8_t* getData() const { return data.get(); }

    uint64_t getCapacity() { return JsonConstant::SCAN_BUFFER_CAPACITY; }
};

struct JsonScanBufferHandle {
    uint64_t bufferIdx;
    JsonScanBuffer buffer;
    uint64_t bufferSize;

    JsonScanBufferHandle(uint64_t buffer_index, JsonScanBuffer&& buffer, uint64_t bufferSize);
};

struct JsonFileHandle {
    std::unique_ptr<common::FileInfo> fileInfo;
    uint64_t filesSize;
    uint64_t readPosition;
    std::atomic<uint64_t> requestedReads;
    std::atomic<uint64_t> actualReads;
    std::atomic<bool> lastReadRequested;

    explicit JsonFileHandle(std::unique_ptr<common::FileInfo> fileInfo);

    bool isLastReadRequested() const { return lastReadRequested; }

    common::FileInfo* getFileInfo() { return fileInfo.get(); }

    bool getPositionAndSize(uint64_t& position, uint64_t& size, uint64_t requestedSize);

    void readAtPosition(uint8_t* pointer, uint64_t size, uint64_t position, bool& finishFile);
};

struct BufferedJSONReaderOptions {
    JsonScanFormat format = JsonScanFormat::UNSTRUCTURED;

    explicit BufferedJSONReaderOptions(JsonScanFormat format) : format{format} {}
};

class BufferedJsonReader {
public:
    BufferedJsonReader(main::ClientContext& context, std::string fileName,
        BufferedJSONReaderOptions options);

    JsonScanFormat getFormat() const { return options.format; }

public:
    void insertBuffer(uint64_t bufferIdx, std::unique_ptr<JsonScanBufferHandle>&& buffer) {
        std::lock_guard<std::mutex> mtx(lock);
        bufferMap.insert(make_pair(bufferIdx, std::move(buffer)));
    }

    uint64_t getBufferIdx() {
        buffer_line_or_object_counts.push_back(-1);
        return bufferIdx++;
    }
    void throwParseError(yyjson_read_err& err, const std::string& extra = "") const;

public:
    main::ClientContext& context;
    std::string fileName;
    BufferedJSONReaderOptions options;
    std::unique_ptr<JsonFileHandle> fileHandle;
    uint64_t bufferIdx;
    std::unordered_map<uint64_t, std::unique_ptr<JsonScanBufferHandle>> bufferMap;
    std::vector<int64_t> buffer_line_or_object_counts;
    bool thrown;

public:
    std::mutex lock;
};

} // namespace json_extension
} // namespace kuzu
