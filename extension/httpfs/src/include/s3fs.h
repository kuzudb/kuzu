#include "httpfs.h"

namespace kuzu {
namespace httpfs {

struct S3AuthParams {
    std::string accessKeyID;
    std::string secretAccessKey;
    std::string endpoint;
    std::string urlStyle;
    std::string region;
};

class S3FileInfo final : public HTTPFileInfo {
public:
    S3FileInfo(std::string path, const common::FileSystem* fileSystem, int flags,
        const S3AuthParams& authParams);

    void initializeClient() override;

    S3AuthParams& getAuthParams() { return authParams; }

private:
    S3AuthParams authParams;
};

struct ParsedS3URL {
    const std::string httpProto;
    const std::string prefix;
    const std::string host;
    const std::string bucket;
    const std::string path;
    const std::string queryParam;
    const std::string trimmedS3URL;

    std::string getHTTPURL() const;
};

class S3FileSystem final : public HTTPFileSystem {
private:
    static constexpr char PAYLOAD_HASH[] =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

public:
    std::unique_ptr<common::FileInfo> openFile(const std::string& path, int flags,
        main::ClientContext* context = nullptr,
        common::FileLockType lock_type = common::FileLockType::NO_LOCK) const override;

    bool canHandleFile(const std::string& path) const override;

    static std::string encodeURL(const std::string& input, bool encodeSlash = false);

    static ParsedS3URL parseS3URL(std::string url, S3AuthParams& params);

protected:
    std::unique_ptr<HTTPResponse> headRequest(
        common::FileInfo* fileInfo, std::string url, HeaderMap headerMap) const override;

    std::unique_ptr<HTTPResponse> getRangeRequest(common::FileInfo* fileInfo,
        const std::string& url, HeaderMap headerMap, uint64_t fileOffset, char* buffer,
        uint64_t bufferLen) const override;

private:
    static HeaderMap createS3Header(std::string url, std::string query, std::string host,
        std::string service, std::string method, const S3AuthParams& authParams);
};

} // namespace httpfs
} // namespace kuzu
