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

struct S3UploadParams {
    uint64_t maxFileSize;
    uint64_t maxNumPartsPerFile;
    uint64_t maxUploadThreads;
};

struct AWSEnvironmentCredentialsProvider {
    static constexpr const char* S3_ACCESS_KEY_ENV_VAR = "S3_ACCESS_KEY_ID";
    static constexpr const char* S3_SECRET_KEY_ENV_VAR = "S3_SECRET_ACCESS_KEY";
    static constexpr const char* S3_ENDPOINT_ENV_VAR = "S3_ENDPOINT";
    static constexpr const char* S3_REGION_ENV_VAR = "S3_REGION";

    static void setOptionValue(main::ClientContext* context);
};

struct S3WriteBuffer {
    S3WriteBuffer(uint16_t partID, uint64_t startOffset, uint64_t size);

    uint8_t* getData() const { return data.get(); }

    uint16_t partID;
    uint64_t numBytesWritten;
    uint64_t startOffset;
    uint64_t endOffset;
    std::unique_ptr<uint8_t[]> data;
    std::atomic<bool> uploading;
};

struct S3FileInfo final : public HTTPFileInfo {
    // AWS requires that part size must be between 5 MiB to 5 GiB:
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
    static constexpr uint64_t AWS_MINIMUM_PART_SIZE = 5242880;

    S3FileInfo(std::string path, common::FileSystem* fileSystem, int flags,
        const S3AuthParams& authParams, const S3UploadParams& uploadParams);

    ~S3FileInfo() override;

    void initialize() override;

    void initializeClient() override;

    std::shared_ptr<S3WriteBuffer> getBuffer(uint16_t writeBufferIdx);

    void rethrowIOError();

    S3AuthParams authParams;
    S3UploadParams uploadParams;

    // Upload related parameters
    uint64_t partSize;
    std::string multipartUploadID;

    // Synchronization for write buffers.
    std::mutex writeBuffersLock;
    std::unordered_map<uint16_t, std::shared_ptr<S3WriteBuffer>> writeBuffers;

    // Synchronization for upload threads.
    std::mutex uploadsInProgressLock;
    std::condition_variable uploadsInProgressCV;
    uint16_t uploadsInProgress;

    // Synchronization for part etags. S3 api returns an etag for each uploaded part.
    // They will later be used to verify the integrity of the uploaded parts.
    std::mutex partEtagsLock;
    std::unordered_map<uint16_t, std::string> partEtags;

    // Upload info.
    std::atomic<uint16_t> numPartsUploaded;
    bool uploadFinalized;

    // If an exception has occurred during upload, we save it in the uploadException.
    std::atomic<bool> uploaderHasException;
    std::exception_ptr uploadException;
};

struct ParsedS3URL {
    const std::string httpProto;
    const std::string prefix;
    const std::string host;
    const std::string bucket;
    const std::string path;
    const std::string queryParam;
    const std::string trimmedS3URL;

    std::string getHTTPURL(std::string httpQueryString = "") const;
};

class S3FileSystem final : public HTTPFileSystem {
private:
    static constexpr char NULL_PAYLOAD_HASH[] =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    static constexpr uint64_t DEFAULT_RESPONSE_BUFFER_SIZE = 1000;
    static constexpr char uploadIDOpenTag[] = "<UploadId>";
    static constexpr char uploadIDCloseTag[] = "</UploadId>";

public:
    std::unique_ptr<common::FileInfo> openFile(const std::string& path, int flags,
        main::ClientContext* context = nullptr,
        common::FileLockType lock_type = common::FileLockType::NO_LOCK) override;

    std::vector<std::string> glob(
        main::ClientContext* context, const std::string& path) const override;

    bool canHandleFile(const std::string& path) const override;

    static std::string encodeURL(const std::string& input, bool encodeSlash = false);

    static std::string decodeURL(std::string input);

    static ParsedS3URL parseS3URL(std::string url, S3AuthParams& params);

    std::string initializeMultiPartUpload(S3FileInfo* fileInfo) const;

    void writeFile(common::FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes,
        uint64_t offset) const override;

    std::shared_ptr<S3WriteBuffer> allocateWriteBuffer(
        uint16_t writeBufferIdx, uint64_t partSize, uint16_t maxThreads);

    void flushAllBuffers(S3FileInfo* fileInfo);

    void finalizeMultipartUpload(S3FileInfo* fileInfo);

    static HeaderMap createS3Header(std::string url, std::string query, std::string host,
        std::string service, std::string method, const S3AuthParams& authParams,
        std::string payloadHash = "", std::string contentType = "");

protected:
    std::unique_ptr<HTTPResponse> headRequest(
        common::FileInfo* fileInfo, const std::string& url, HeaderMap headerMap) const override;

    std::unique_ptr<HTTPResponse> getRangeRequest(common::FileInfo* fileInfo,
        const std::string& url, HeaderMap headerMap, uint64_t fileOffset, char* buffer,
        uint64_t bufferLen) const override;

    std::unique_ptr<HTTPResponse> postRequest(common::FileInfo* fileInfo, const std::string& url,
        HeaderMap headerMap, std::unique_ptr<uint8_t[]>& outputBuffer, uint64_t& outputBufferLen,
        const uint8_t* inputBuffer, uint64_t inputBufferLen, std::string params) const override;

    std::unique_ptr<HTTPResponse> putRequest(common::FileInfo* fileInfo, const std::string& url,
        HeaderMap headerMap, const uint8_t* inputBuffer, uint64_t inputBufferLen,
        std::string httpParams = "") const override;

private:
    static std::string getPayloadHash(const uint8_t* buffer, uint64_t bufferLen);

    void flushBuffer(S3FileInfo* fileInfo, std::shared_ptr<S3WriteBuffer> bufferToFlush) const;

    static void uploadBuffer(S3FileInfo* fileInfo, std::shared_ptr<S3WriteBuffer> bufferToUpload);

    static std::string getUploadID(const std::string& response);

private:
    std::mutex bufferInfoLock;
    std::condition_variable bufferInfoCV;
    uint16_t numUsedBuffers = 0;
};

struct AWSListObjectV2 {
    static constexpr char OPEN_KEY_TAG[] = "<Key>";
    static constexpr char CLOSE_KEY_TAG[] = "</Key>";
    static constexpr char OPEN_CONTINUATION_TAG[] = "<NextContinuationToken>";
    static constexpr char CLOSE_CONTINUATION_TAG[] = "</NextContinuationToken>";
    static constexpr char OPEN_COMMON_PREFIX_TAG[] = "<CommonPrefixes>";
    static constexpr char OPEN_PREFIX_TAG[] = "<Prefix>";
    static constexpr char CLOSE_PREFIX_TAG[] = "</Prefix>";

    static std::string request(
        std::string& path, S3AuthParams& authParams, std::string& continuationToken);
    static void parseKey(std::string& awsResponse, std::vector<std::string>& result);
    static std::vector<std::string> parseCommonPrefix(std::string& awsResponse);
    static std::string parseContinuationToken(std::string& awsResponse);
};

} // namespace httpfs
} // namespace kuzu
