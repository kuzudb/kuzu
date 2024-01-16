#include "s3fs.h"

#include "common/cast.h"
#include "common/exception/io.h"
#include "common/types/timestamp_t.h"
#include "crypto.h"
#include "main/client_context.h"

namespace kuzu {
namespace httpfs {

using namespace kuzu::common;

S3FileInfo::S3FileInfo(
    std::string path, common::FileSystem* fileSystem, int flags, const S3AuthParams& authParams)
    : HTTPFileInfo{std::move(path), fileSystem, flags}, authParams{authParams} {}

void S3FileInfo::initializeClient() {
    auto parsedURL = S3FileSystem::parseS3URL(path, authParams);
    auto protoHostPort = parsedURL.httpProto + parsedURL.host;
    httpClient = HTTPFileSystem::getClient(protoHostPort);
}

std::string ParsedS3URL::getHTTPURL() const {
    return httpProto + host + S3FileSystem::encodeURL(path);
}

S3AuthParams getS3AuthParams(main::ClientContext* context) {
    S3AuthParams authParams;
    authParams.accessKeyID = context->getCurrentSetting("s3_access_key_id");
    authParams.secretAccessKey = context->getCurrentSetting("s3_secret_access_key");
    authParams.endpoint = context->getCurrentSetting("s3_endpoint");
    authParams.urlStyle = context->getCurrentSetting("s3_url_style");
    authParams.region = context->getCurrentSetting("s3_region");
    return authParams;
}

std::unique_ptr<common::FileInfo> S3FileSystem::openFile(const std::string& path, int flags,
    main::ClientContext* context, common::FileLockType /*lock_type*/) {
    auto authParams = getS3AuthParams(context);
    auto s3FileInfo = std::make_unique<S3FileInfo>(path, this, flags, std::move(authParams));
    s3FileInfo->initialize();
    return std::move(s3FileInfo);
}

bool S3FileSystem::canHandleFile(const std::string& path) {
    return path.rfind("s3://", 0) == 0;
}

std::string S3FileSystem::encodeURL(const std::string& input, bool encodeSlash) {
    static const char* hex_digit = "0123456789ABCDEF";
    std::string result;
    result.reserve(input.size());
    for (auto i = 0u; i < input.length(); i++) {
        char ch = input[i];
        if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
            ch == '_' || ch == '-' || ch == '~' || ch == '.') {
            result += ch;
        } else if (ch == '/') {
            if (encodeSlash) {
                result += std::string("%2F");
            } else {
                result += ch;
            }
        } else {
            result += std::string("%");
            result += hex_digit[static_cast<unsigned char>(ch) >> 4];
            result += hex_digit[static_cast<unsigned char>(ch) & 15];
        }
    }
    return result;
}

static std::string getPrefix(std::string url) {
    // TODO(Ziyi): support more filesystems (gcp, azure, etc.)
    const std::string prefixes[] = {"s3://"};
    for (auto& prefix : prefixes) {
        if (url.starts_with(prefix)) {
            return prefix;
        }
    }
    throw IOException("URL needs to start with s3://.");
}

ParsedS3URL S3FileSystem::parseS3URL(std::string url, S3AuthParams& params) {
    std::string httpProto, prefix, host, bucket, path, queryParameters, trimmedS3URL;

    prefix = getPrefix(url);
    auto prefixEndPos = url.find("//") + 2;
    auto slashPos = url.find('/', prefixEndPos);
    if (slashPos == std::string::npos) {
        throw IOException("URL needs to contain a '/' after the host");
    }
    bucket = url.substr(prefixEndPos, slashPos - prefixEndPos);
    if (bucket.empty()) {
        throw IOException("URL needs to contain a bucket name");
    }

    if (params.urlStyle == "path") {
        path = "/" + bucket;
    } else {
        path = "";
    }

    auto parameterPos = url.find_first_of('?');
    if (parameterPos != std::string::npos) {
        queryParameters = url.substr(parameterPos + 1);
        trimmedS3URL = url.substr(0, parameterPos);
    } else {
        trimmedS3URL = url;
    }

    if (!queryParameters.empty()) {
        path += url.substr(slashPos, parameterPos - slashPos);
    } else {
        path += url.substr(slashPos);
    }

    if (path.empty()) {
        throw IOException("URL needs to contain key");
    }

    if (params.urlStyle == "vhost" || params.urlStyle == "") {
        host = bucket + "." + params.endpoint;
    } else {
        host = params.endpoint;
    }

    return {"https://", prefix, host, bucket, path, queryParameters, trimmedS3URL};
}

// Date header is in the format: %Y%m%d.
std::string getDateHeader(const timestamp_t& timestamp) {
    auto date = Timestamp::getDate(timestamp);
    int32_t year, month, day;
    Date::convert(date, year, month, day);
    std::string formatStr = "{}";
    if (month < 10) {
        formatStr += "0";
    }
    formatStr += "{}";
    if (day < 10) {
        formatStr += "0";
    }
    formatStr += "{}";
    return common::stringFormat(formatStr, year, month, day);
}

// Timestamp header is in the format: %Y%m%dT%H%M%SZ.
std::string getDateTimeHeader(const timestamp_t& timestamp) {
    auto formatStr = getDateHeader(timestamp);
    auto time = Timestamp::getTime(timestamp);
    formatStr += "T";
    int32_t hours, minutes, seconds, micros;
    Time::Convert(time, hours, minutes, seconds, micros);
    if (hours < 10) {
        formatStr += "0";
    }
    formatStr += "{}";
    if (minutes < 10) {
        formatStr += "0";
    }
    formatStr += "{}";
    if (seconds < 10) {
        formatStr += "0";
    }
    formatStr += "{}Z";
    return common::stringFormat(formatStr, hours, minutes, seconds);
}

std::unique_ptr<HTTPResponse> S3FileSystem::headRequest(
    common::FileInfo* fileInfo, std::string url, HeaderMap /*headerMap*/) {
    auto& authParams = ku_dynamic_cast<FileInfo*, S3FileInfo*>(fileInfo)->getAuthParams();
    auto parsedS3URL = parseS3URL(url, authParams);
    auto httpURL = parsedS3URL.getHTTPURL();
    auto headers = createS3Header(parsedS3URL.path, "", parsedS3URL.host, "s3", "HEAD", authParams);
    return HTTPFileSystem::headRequest(fileInfo, httpURL, headers);
}

std::unique_ptr<HTTPResponse> S3FileSystem::getRangeRequest(common::FileInfo* fileInfo,
    const std::string& url, HeaderMap /*headerMap*/, uint64_t fileOffset, char* buffer,
    uint64_t bufferLen) {
    auto& authParams = ku_dynamic_cast<FileInfo*, S3FileInfo*>(fileInfo)->getAuthParams();
    auto parsedS3URL = parseS3URL(url, authParams);
    auto s3HTTPUrl = parsedS3URL.getHTTPURL();
    auto headers = createS3Header(parsedS3URL.path, "", parsedS3URL.host, "s3", "GET", authParams);
    return HTTPFileSystem::getRangeRequest(
        fileInfo, s3HTTPUrl, headers, fileOffset, buffer, bufferLen);
}

HeaderMap S3FileSystem::createS3Header(std::string url, std::string query, std::string host,
    std::string service, std::string method, const S3AuthParams& authParams) {
    HeaderMap res;
    res["Host"] = host;
    // If access key is not set, we don't set the headers at all to allow accessing public files
    // through s3 urls.
    if (authParams.secretAccessKey.empty() && authParams.accessKeyID.empty()) {
        return res;
    }
    std::string payloadHash = PAYLOAD_HASH;
    auto timestamp = common::Timestamp::getCurrentTimestamp();
    auto dateHeader = getDateHeader(timestamp);
    auto datetimeHeader = getDateTimeHeader(timestamp);
    res["x-amz-date"] = datetimeHeader;
    res["x-amz-content-sha256"] = payloadHash;
    std::string signedHeaders = "";
    hashBytes canonicalRequestHash;
    hashStr canonicalRequestHashStr;
    signedHeaders += "host;x-amz-content-sha256;x-amz-date";
    auto canonicalRequest = method + "\n" + S3FileSystem::encodeURL(url) + "\n" + query;
    canonicalRequest += "\nhost:" + host + "\nx-amz-content-sha256:" + payloadHash +
                        "\nx-amz-date:" + datetimeHeader;
    canonicalRequest += "\n\n" + signedHeaders + "\n" + payloadHash;
    sha256(canonicalRequest.c_str(), canonicalRequest.length(), canonicalRequestHash);
    hex256(canonicalRequestHash, canonicalRequestHashStr);
    auto stringToSign = "AWS4-HMAC-SHA256\n" + datetimeHeader + "\n" + dateHeader + "/" +
                        authParams.region + "/" + service + "/aws4_request\n" +
                        std::string((char*)canonicalRequestHashStr, sizeof(hashStr));
    hashBytes dateSignature, regionSignature, serviceSignature, signingKeySignature, signature;
    hashStr signatureStr;
    auto signKey = "AWS4" + authParams.secretAccessKey;
    hmac256(dateHeader, signKey.c_str(), signKey.length(), dateSignature);
    hmac256(authParams.region, dateSignature, regionSignature);
    hmac256(service, regionSignature, serviceSignature);
    hmac256("aws4_request", serviceSignature, signingKeySignature);
    hmac256(stringToSign, signingKeySignature, signature);
    hex256(signature, signatureStr);

    res["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + authParams.accessKeyID + "/" +
                           dateHeader + "/" + authParams.region + "/" + service +
                           "/aws4_request, SignedHeaders=" + signedHeaders +
                           ", Signature=" + std::string((char*)signatureStr, sizeof(hashStr));

    return res;
}

} // namespace httpfs
} // namespace kuzu
