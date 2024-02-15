#pragma once

#include "common/case_insensitive_map.h"
#include "common/file_system/file_system.h"
#include "common/string_utils.h"
#include "httplib.h"
#include "main/client_context.h"

#if defined(_WIN32)
#define O_ACCMODE 0x0003
#endif

namespace kuzu {
namespace httpfs {

using HeaderMap = common::case_insensitive_map_t<std::string>;

struct HTTPResponse {
    HTTPResponse(httplib::Response& res, const std::string& url);

    int code;
    std::string error;
    HeaderMap headers;
    std::string url;
    std::string body;
};

struct HTTPParams {
    // TODO(Ziyi): Make them configurable.
    static constexpr uint64_t DEFAULT_TIMEOUT = 30000;
    static constexpr uint64_t DEFAULT_RETRIES = 3;
    static constexpr uint64_t DEFAULT_RETRY_WAIT_MS = 100;
    static constexpr float DEFAULT_RETRY_BACKOFF = 4;
    static constexpr bool DEFAULT_KEEP_ALIVE = true;
};

struct HTTPFileInfo : public common::FileInfo {
    HTTPFileInfo(std::string path, common::FileSystem* fileSystem, int flags);

    virtual void initialize();

    virtual void initializeClient();

    // We keep a http client stored for connection reuse with keep-alive headers.
    std::unique_ptr<httplib::Client> httpClient;

    int flags;
    uint64_t length;
    uint64_t availableBuffer;
    uint64_t bufferIdx;
    uint64_t fileOffset;
    uint64_t bufferStartPos;
    uint64_t bufferEndPos;
    std::unique_ptr<uint8_t[]> readBuffer;
    constexpr static uint64_t READ_BUFFER_LEN = 1000000;
};

class HTTPFileSystem : public common::FileSystem {
    friend struct HTTPFileInfo;

public:
    std::unique_ptr<common::FileInfo> openFile(const std::string& path, int flags,
        main::ClientContext* context = nullptr,
        common::FileLockType lock_type = common::FileLockType::NO_LOCK) override;

    std::vector<std::string> glob(
        main::ClientContext* context, const std::string& path) const override;

    bool canHandleFile(const std::string& path) const override;

    static std::unique_ptr<httplib::Client> getClient(const std::string& host);

    static std::unique_ptr<httplib::Headers> getHTTPHeaders(HeaderMap& headerMap);

protected:
    void readFromFile(common::FileInfo* fileInfo, void* buffer, uint64_t numBytes,
        uint64_t position) const override;

    int64_t readFile(common::FileInfo* fileInfo, void* buf, size_t numBytes) const override;

    int64_t seek(common::FileInfo* fileInfo, uint64_t offset, int whence) const override;

    uint64_t getFileSize(common::FileInfo* fileInfo) const override;

    static std::pair<std::string, std::string> parseUrl(const std::string& url);

    static std::unique_ptr<HTTPResponse> runRequestWithRetry(
        const std::function<httplib::Result(void)>& request, const std::string& url,
        std::string method, const std::function<void(void)>& retry = {});

    virtual std::unique_ptr<HTTPResponse> headRequest(
        common::FileInfo* fileInfo, const std::string& url, HeaderMap headerMap) const;

    virtual std::unique_ptr<HTTPResponse> getRangeRequest(common::FileInfo* fileInfo,
        const std::string& url, HeaderMap headerMap, uint64_t fileOffset, char* buffer,
        uint64_t bufferLen) const;

    virtual std::unique_ptr<HTTPResponse> postRequest(common::FileInfo* fileInfo,
        const std::string& url, HeaderMap headerMap, std::unique_ptr<uint8_t[]>& outputBuffer,
        uint64_t& outputBufferLen, const uint8_t* inputBuffer, uint64_t inputBufferLen,
        std::string params = "") const;

    virtual std::unique_ptr<HTTPResponse> putRequest(common::FileInfo* fileInfo,
        const std::string& url, HeaderMap headerMap, const uint8_t* inputBuffer,
        uint64_t inputBufferLen, std::string params = "") const;
};

} // namespace httpfs
} // namespace kuzu
