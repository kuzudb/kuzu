#include "s3fs.h"

#include "common/cast.h"
#include "common/exception/io.h"
#include "common/types/timestamp_t.h"
#include "crypto.h"
#include "main/client_context.h"

namespace kuzu {
namespace httpfs {

using namespace kuzu::common;

void AWSEnvironmentCredentialsProvider::setOptionValue(main::ClientContext* context) {
    auto accessKeyID = context->getEnvVariable(S3_ACCESS_KEY_ENV_VAR);
    auto secretAccessKey = context->getEnvVariable(S3_SECRET_KEY_ENV_VAR);
    auto endpoint = context->getEnvVariable(S3_ENDPOINT_ENV_VAR);
    auto region = context->getEnvVariable(S3_REGION_ENV_VAR);
    if (accessKeyID != "") {
        context->setExtensionOption("s3_access_key_id", Value::createValue(accessKeyID));
    }
    if (secretAccessKey != "") {
        context->setExtensionOption("s3_secret_access_key", Value::createValue(secretAccessKey));
    }
    if (endpoint != "") {
        context->setExtensionOption("s3_endpoint", Value::createValue(endpoint));
    }
    if (region != "") {
        context->setExtensionOption("s3_region", Value::createValue(region));
    }
}

S3WriteBuffer::S3WriteBuffer(uint16_t partID, uint64_t startOffset, uint64_t size)
    : partID{partID}, numBytesWritten{0}, startOffset{startOffset}, uploading{false} {
    data = std::make_unique<uint8_t[]>(size);
    endOffset = startOffset + size;
    partID = startOffset / size;
}

S3FileInfo::S3FileInfo(std::string path, common::FileSystem* fileSystem, int flags,
    const S3AuthParams& authParams, const S3UploadParams& uploadParams)
    : HTTPFileInfo{std::move(path), fileSystem, flags}, authParams{authParams},
      uploadParams{uploadParams}, uploadsInProgress{0}, numPartsUploaded{0}, uploadFinalized{false},
      uploaderHasException{false} {}

S3FileInfo::~S3FileInfo() {
    auto s3fs = ku_dynamic_cast<FileSystem*, S3FileSystem*>(fileSystem);
    if ((((flags & O_ACCMODE) & O_WRONLY)) && !uploadFinalized) {
        s3fs->flushAllBuffers(this);
        if (numPartsUploaded) {
            s3fs->finalizeMultipartUpload(this);
        }
    }
}

void S3FileInfo::initialize() {
    HTTPFileInfo::initialize();
    auto s3fs = ku_dynamic_cast<const common::FileSystem*, const S3FileSystem*>(fileSystem);
    if ((flags & O_ACCMODE) & O_WRONLY) {
        auto maxNumParts = uploadParams.maxNumPartsPerFile;
        auto requiredPartSize = uploadParams.maxFileSize / maxNumParts;
        partSize = std::max<uint64_t>(AWS_MINIMUM_PART_SIZE, requiredPartSize);
        KU_ASSERT(partSize * maxNumParts >= uploadParams.maxFileSize);
        multipartUploadID = s3fs->initializeMultiPartUpload(this);
    }
}

void S3FileInfo::initializeClient() {
    auto parsedURL = S3FileSystem::parseS3URL(path, authParams);
    auto protoHostPort = parsedURL.httpProto + parsedURL.host;
    httpClient = HTTPFileSystem::getClient(protoHostPort);
}

std::shared_ptr<S3WriteBuffer> S3FileInfo::getBuffer(uint16_t writeBufferIdx) {
    std::unique_lock<std::mutex> lck(writeBuffersLock);
    auto s3FS = ku_dynamic_cast<common::FileSystem*, S3FileSystem*>(fileSystem);
    if (writeBuffers.contains(writeBufferIdx)) {
        return writeBuffers.at(writeBufferIdx);
    }
    auto writeBuffer =
        s3FS->allocateWriteBuffer(writeBufferIdx, partSize, uploadParams.maxUploadThreads);
    writeBuffers.emplace(writeBufferIdx, std::move(writeBuffer));
    return writeBuffers.at(writeBufferIdx);
}

void S3FileInfo::rethrowIOError() {
    if (uploaderHasException) {
        std::rethrow_exception(uploadException);
    }
}

std::string ParsedS3URL::getHTTPURL(std::string httpQueryString) const {
    auto url = httpProto + host + S3FileSystem::encodeURL(path);
    if (!httpQueryString.empty()) {
        url += "?" + httpQueryString;
    }
    return url;
}

S3AuthParams getS3AuthParams(main::ClientContext* context) {
    S3AuthParams authParams;
    authParams.accessKeyID = context->getCurrentSetting("s3_access_key_id").toString();
    authParams.secretAccessKey = context->getCurrentSetting("s3_secret_access_key").toString();
    authParams.endpoint = context->getCurrentSetting("s3_endpoint").toString();
    authParams.urlStyle = context->getCurrentSetting("s3_url_style").toString();
    authParams.region = context->getCurrentSetting("s3_region").toString();
    return authParams;
}

S3UploadParams getS3UploadParams(main::ClientContext* context) {
    S3UploadParams uploadParams;
    uploadParams.maxFileSize =
        context->getCurrentSetting("s3_uploader_max_filesize").getValue<int64_t>();
    uploadParams.maxNumPartsPerFile =
        context->getCurrentSetting("s3_uploader_max_num_parts_per_file").getValue<int64_t>();
    uploadParams.maxUploadThreads =
        context->getCurrentSetting("s3_uploader_threads_limit").getValue<int64_t>();
    return uploadParams;
}

std::unique_ptr<common::FileInfo> S3FileSystem::openFile(const std::string& path, int flags,
    main::ClientContext* context, common::FileLockType /*lock_type*/) {
    auto authParams = getS3AuthParams(context);
    auto uploadParams = getS3UploadParams(context);
    auto s3FileInfo = std::make_unique<S3FileInfo>(path, this, flags, authParams, uploadParams);
    s3FileInfo->initialize();
    return std::move(s3FileInfo);
}

bool likes(const char* string, uint64_t slen, const char* pattern, uint64_t plen) {
    uint64_t sidx = 0;
    uint64_t pidx = 0;
main_loop : {
    // main matching loop
    while (sidx < slen && pidx < plen) {
        char s = string[sidx];
        char p = pattern[pidx];
        switch (p) {
        case '*': {
            // asterisk: match any set of characters
            // skip any subsequent asterisks
            pidx++;
            while (pidx < plen && pattern[pidx] == '*') {
                pidx++;
            }
            // if the asterisk is the last character, the pattern always matches
            if (pidx == plen) {
                return true;
            }
            // recursively match the remainder of the pattern
            for (; sidx < slen; sidx++) {
                if (likes(string + sidx, slen - sidx, pattern + pidx, plen - pidx)) {
                    return true;
                }
            }
            return false;
        }
        case '?':
            break;
        case '[':
            pidx++;
            goto parse_bracket;
        case '\\':
            // escape character, next character needs to match literally
            pidx++;
            // check that we still have a character remaining
            if (pidx == plen) {
                return false;
            }
            p = pattern[pidx];
            if (s != p) {
                return false;
            }
            break;
        default:
            // not a control character: characters need to match literally
            if (s != p) {
                return false;
            }
            break;
        }
        sidx++;
        pidx++;
    }
    while (pidx < plen && pattern[pidx] == '*') {
        pidx++;
    }
    // we are finished only if we have consumed the full pattern
    return pidx == plen && sidx == slen;
}
parse_bracket : {
    // inside a bracket
    if (pidx == plen) {
        return false;
    }
    // check the first character
    // if it is an exclamation mark we need to invert our logic
    char p = pattern[pidx];
    char s = string[sidx];
    bool invert = false;
    if (p == '!') {
        invert = true;
        pidx++;
    }
    bool found_match = invert;
    uint64_t start_pos = pidx;
    bool found_closing_bracket = false;
    // now check the remainder of the pattern
    while (pidx < plen) {
        p = pattern[pidx];
        // if the first character is a closing bracket, we match it literally
        // otherwise it indicates an end of bracket
        if (p == ']' && pidx > start_pos) {
            // end of bracket found: we are done
            found_closing_bracket = true;
            pidx++;
            break;
        }
        // we either match a range (a-b) or a single character (a)
        // check if the next character is a dash
        if (pidx + 1 == plen) {
            // no next character!
            break;
        }
        bool matches;
        if (pattern[pidx + 1] == '-') {
            // range! find the next character in the range
            if (pidx + 2 == plen) {
                break;
            }
            char next_char = pattern[pidx + 2];
            // check if the current character is within the range
            matches = s >= p && s <= next_char;
            // shift the pattern forward past the range
            pidx += 3;
        } else {
            // no range! perform a direct match
            matches = p == s;
            // shift the pattern forward past the character
            pidx++;
        }
        if (found_match == invert && matches) {
            // found a match! set the found_matches flag
            // we keep on pattern matching after this until we reach the end bracket
            // however, we don't need to update the found_match flag anymore
            found_match = !invert;
        }
    }
    if (!found_closing_bracket) {
        // no end of bracket: invalid pattern
        return false;
    }
    if (!found_match) {
        // did not match the bracket: return false;
        return false;
    }
    // finished the bracket matching: move forward
    sidx++;
    goto main_loop;
}
}

static bool match(std::vector<std::string>::const_iterator key,
    std::vector<std::string>::const_iterator key_end,
    std::vector<std::string>::const_iterator pattern,
    std::vector<std::string>::const_iterator pattern_end) {

    while (key != key_end && pattern != pattern_end) {
        if (*pattern == "**") {
            if (std::next(pattern) == pattern_end) {
                return true;
            }
            while (key != key_end) {
                if (match(key, key_end, std::next(pattern), pattern_end)) {
                    return true;
                }
                key++;
            }
            return false;
        }
        if (!likes(key->data(), key->length(), pattern->data(), pattern->length())) {
            return false;
        }
        key++;
        pattern++;
    }
    return key == key_end && pattern == pattern_end;
}

std::vector<std::string> S3FileSystem::glob(
    main::ClientContext* context, const std::string& path) const {
    auto s3AuthParams = getS3AuthParams(context);
    auto parsedS3URL = parseS3URL(path, s3AuthParams);
    auto parsedGlobURL = parsedS3URL.trimmedS3URL;
    auto firstWildcardPos = parsedGlobURL.find_first_of("*[\\");
    if (firstWildcardPos == std::string::npos) {
        return {path};
    }
    auto sharedPath = parsedGlobURL.substr(0, firstWildcardPos);
    std::vector<std::string> s3Keys;
    std::string mainContinuationToken = "";
    do {
        std::string response =
            AWSListObjectV2::request(sharedPath, s3AuthParams, mainContinuationToken);
        mainContinuationToken = AWSListObjectV2::parseContinuationToken(response);
        AWSListObjectV2::parseKey(response, s3Keys);
        // Repeat requests until the keys of all common prefixes are parsed.
        auto commonPrefixes = AWSListObjectV2::parseCommonPrefix(response);
        while (!commonPrefixes.empty()) {
            auto prefixPath = parsedS3URL.prefix + parsedS3URL.bucket + '/' + commonPrefixes.back();
            commonPrefixes.pop_back();
            std::string commonPrefixContinuationToken = "";
            do {
                auto prefixRequest = AWSListObjectV2::request(
                    prefixPath, s3AuthParams, commonPrefixContinuationToken);
                AWSListObjectV2::parseKey(prefixRequest, s3Keys);
                auto commonPrefixesToInsert = AWSListObjectV2::parseCommonPrefix(prefixRequest);
                commonPrefixes.insert(commonPrefixes.end(), commonPrefixesToInsert.begin(),
                    commonPrefixesToInsert.end());
                commonPrefixContinuationToken =
                    AWSListObjectV2::parseContinuationToken(prefixRequest);
            } while (!commonPrefixContinuationToken.empty());
        }
    } while (!mainContinuationToken.empty());
    auto trimmedPattern = parsedS3URL.path.substr(1);
    if (s3AuthParams.urlStyle == "path") {
        trimmedPattern = trimmedPattern.substr(parsedS3URL.bucket.length() + 1);
    }
    std::vector<std::string> patternSplits = common::StringUtils::split(trimmedPattern, "/");
    std::vector<std::string> globResult;
    for (const auto& s3Key : s3Keys) {
        std::vector<std::string> splitKeys = common::StringUtils::split(s3Key, "/");
        bool isMatch =
            match(splitKeys.begin(), splitKeys.end(), patternSplits.begin(), patternSplits.end());
        if (isMatch) {
            globResult.push_back(parsedS3URL.prefix + parsedS3URL.bucket + "/" + s3Key);
        }
    }
    return globResult;
}

bool S3FileSystem::canHandleFile(const std::string& path) const {
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

std::string S3FileSystem::decodeURL(std::string input) {
    std::string result;
    result.reserve(input.size());
    char ch;
    replace(input.begin(), input.end(), '+', ' ');
    for (auto i = 0u; i < input.length(); i++) {
        if (int(input[i]) == 37 /* % */) {
            unsigned int ii;
            sscanf(input.substr(i + 1, 2).c_str(), "%x", &ii);
            ch = static_cast<char>(ii);
            result += ch;
            i += 2;
        } else {
            result += input[i];
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

std::string S3FileSystem::initializeMultiPartUpload(S3FileInfo* fileInfo) const {
    // AWS response is around 300~ chars in docs so this should be enough to not need a resize.
    auto responseBufferLen = DEFAULT_RESPONSE_BUFFER_SIZE;
    auto responseBuffer = std::make_unique<uint8_t[]>(responseBufferLen);
    std::string queryParam = "uploads=";
    auto res = postRequest(fileInfo, fileInfo->path, {}, responseBuffer, responseBufferLen,
        nullptr /* inputBuffer */, 0 /* inputBufferLen */, queryParam);
    std::string result(reinterpret_cast<const char*>(responseBuffer.get()), responseBufferLen);
    return getUploadID(result);
}

void S3FileSystem::writeFile(
    common::FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes, uint64_t offset) const {
    auto s3FileInfo = ku_dynamic_cast<FileInfo*, S3FileInfo*>(fileInfo);
    if (!((s3FileInfo->flags & O_ACCMODE) & O_WRONLY)) {
        throw IOException("Write called on a file which is not open in write mode.");
    }
    uint64_t numBytesWritten = 0;
    while (numBytesWritten < numBytes) {
        auto currOffset = offset + numBytesWritten;
        // We use amazon multipart upload API which segments an object into a set of parts. Since we
        // don't track the usage of individual part, determining whether we can upload a part is
        // challenging if we allow non-sequential write.
        if (currOffset != s3FileInfo->fileOffset) {
            throw InternalException("Non-sequential write not supported.");
        }
        auto writeBufferIdx = currOffset / s3FileInfo->partSize;
        auto writeBuffer = s3FileInfo->getBuffer(writeBufferIdx);
        auto offsetToWrite = currOffset - writeBuffer->startOffset;
        auto numBytesToWrite =
            std::min<uint64_t>(numBytes - numBytesWritten, s3FileInfo->partSize - offsetToWrite);
        memcpy(writeBuffer->getData() + offsetToWrite, buffer + numBytesWritten, numBytesToWrite);
        writeBuffer->numBytesWritten += numBytesToWrite;
        if (writeBuffer->numBytesWritten >= s3FileInfo->partSize) {
            flushBuffer(s3FileInfo, writeBuffer);
        }
        s3FileInfo->fileOffset += numBytesToWrite;
        numBytesWritten += numBytesToWrite;
    }
}

std::shared_ptr<S3WriteBuffer> S3FileSystem::allocateWriteBuffer(
    uint16_t writeBufferIdx, uint64_t partSize, uint16_t maxThreads) {
    std::unique_lock<std::mutex> lck(bufferInfoLock);
    if (numUsedBuffers >= maxThreads) {
        bufferInfoCV.wait(lck, [&] { return numUsedBuffers < maxThreads; });
    }
    numUsedBuffers++;
    return std::make_shared<S3WriteBuffer>(writeBufferIdx, writeBufferIdx * partSize, partSize);
}

void S3FileSystem::flushAllBuffers(S3FileInfo* fileInfo) {
    //  Collect references to all buffers to check
    std::vector<std::shared_ptr<S3WriteBuffer>> buffersToFlush;
    fileInfo->writeBuffersLock.lock();
    for (auto [bufferIdx, writeBuffer] : fileInfo->writeBuffers) {
        buffersToFlush.push_back(writeBuffer);
    }
    fileInfo->writeBuffersLock.unlock();
    for (auto& write_buffer : buffersToFlush) {
        if (!write_buffer->uploading) {
            flushBuffer(fileInfo, write_buffer);
        }
    }
    std::unique_lock<std::mutex> lck(fileInfo->uploadsInProgressLock);
    fileInfo->uploadsInProgressCV.wait(
        lck, [fileInfo] { return fileInfo->uploadsInProgress == 0; });
    fileInfo->rethrowIOError();
}

std::stringstream getFinalizeUploadQueryBody(S3FileInfo* fileInfo) {
    std::stringstream ss;
    ss << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
    auto parts = fileInfo->numPartsUploaded.load();
    for (auto i = 0; i < parts; i++) {
        auto etag_lookup = fileInfo->partEtags.find(i);
        if (etag_lookup == fileInfo->partEtags.end()) {
            throw IOException("Unknown part number");
        }
        ss << "<Part><ETag>" << etag_lookup->second << "</ETag><PartNumber>" << i + 1
           << "</PartNumber></Part>";
    }
    ss << "</CompleteMultipartUpload>";
    return ss;
}

static void verifyUploadResult(const std::string& result, const HTTPResponse& response) {
    auto openUploadResultTagPos = result.find("<CompleteMultipartUploadResult", 0);
    if (openUploadResultTagPos == std::string::npos) {
        throw IOException(common::stringFormat(
            "Unexpected response during S3 multipart upload finalization: {}\n\n{}", response.code,
            result));
    }
}

void S3FileSystem::finalizeMultipartUpload(S3FileInfo* fileInfo) {
    auto s3FS =
        ku_dynamic_cast<const common::FileSystem*, const S3FileSystem*>(fileInfo->fileSystem);
    fileInfo->uploadFinalized = true;
    auto finalizeUploadQueryBody = getFinalizeUploadQueryBody(fileInfo);
    auto body = finalizeUploadQueryBody.str();
    uint64_t responseBufferSize = DEFAULT_RESPONSE_BUFFER_SIZE;
    auto responseBuffer = std::make_unique<uint8_t[]>(responseBufferSize);
    std::string queryParam =
        "uploadId=" + S3FileSystem::encodeURL(fileInfo->multipartUploadID, true);
    auto res = s3FS->postRequest(fileInfo, fileInfo->path, {} /* headerMap */, responseBuffer,
        responseBufferSize, reinterpret_cast<const uint8_t*>(body.c_str()), body.length(),
        queryParam);
    std::string result(reinterpret_cast<char*>(responseBuffer.get()), responseBufferSize);
    verifyUploadResult(result, *res);
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

HeaderMap S3FileSystem::createS3Header(std::string url, std::string query, std::string host,
    std::string service, std::string method, const S3AuthParams& authParams,
    std::string payloadHash, std::string contentType) {
    HeaderMap res;
    res["Host"] = host;
    // If access key is not set, we don't set the headers at all to allow accessing public files
    // through s3 urls.
    if (authParams.secretAccessKey.empty() && authParams.accessKeyID.empty()) {
        return res;
    }
    if (payloadHash.empty()) {
        payloadHash = NULL_PAYLOAD_HASH;
    }
    auto timestamp = common::Timestamp::getCurrentTimestamp();
    auto dateHeader = getDateHeader(timestamp);
    auto datetimeHeader = getDateTimeHeader(timestamp);
    res["x-amz-date"] = datetimeHeader;
    res["x-amz-content-sha256"] = payloadHash;
    std::string signedHeaders = "";
    hash_bytes canonicalRequestHash;
    hash_str canonicalRequestHashStr;
    if (!contentType.empty()) {
        signedHeaders += "content-type;";
    }
    signedHeaders += "host;x-amz-content-sha256;x-amz-date";
    auto canonicalRequest = method + "\n" + S3FileSystem::encodeURL(url) + "\n" + query;
    if (!contentType.empty()) {
        canonicalRequest += "\ncontent-type:" + contentType;
    }
    canonicalRequest += "\nhost:" + host + "\nx-amz-content-sha256:" + payloadHash +
                        "\nx-amz-date:" + datetimeHeader;
    canonicalRequest += "\n\n" + signedHeaders + "\n" + payloadHash;
    sha256(canonicalRequest.c_str(), canonicalRequest.length(), canonicalRequestHash);
    hex256(canonicalRequestHash, canonicalRequestHashStr);
    auto stringToSign = "AWS4-HMAC-SHA256\n" + datetimeHeader + "\n" + dateHeader + "/" +
                        authParams.region + "/" + service + "/aws4_request\n" +
                        std::string((char*)canonicalRequestHashStr, sizeof(hash_str));
    hash_bytes dateSignature, regionSignature, serviceSignature, signingKeySignature, signature;
    hash_str signatureStr;
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
                           ", Signature=" + std::string((char*)signatureStr, sizeof(hash_str));

    return res;
}

std::unique_ptr<HTTPResponse> S3FileSystem::headRequest(
    common::FileInfo* fileInfo, const std::string& url, HeaderMap /*headerMap*/) const {
    auto& authParams = ku_dynamic_cast<FileInfo*, S3FileInfo*>(fileInfo)->authParams;
    auto parsedS3URL = parseS3URL(url, authParams);
    auto httpURL = parsedS3URL.getHTTPURL();
    auto headers = createS3Header(parsedS3URL.path, "", parsedS3URL.host, "s3", "HEAD", authParams);
    return HTTPFileSystem::headRequest(fileInfo, httpURL, headers);
}

std::unique_ptr<HTTPResponse> S3FileSystem::getRangeRequest(common::FileInfo* fileInfo,
    const std::string& url, HeaderMap /*headerMap*/, uint64_t fileOffset, char* buffer,
    uint64_t bufferLen) const {
    auto& authParams = ku_dynamic_cast<FileInfo*, S3FileInfo*>(fileInfo)->authParams;
    auto parsedS3URL = parseS3URL(url, authParams);
    auto s3HTTPUrl = parsedS3URL.getHTTPURL();
    auto headers = createS3Header(parsedS3URL.path, "", parsedS3URL.host, "s3", "GET", authParams);
    return HTTPFileSystem::getRangeRequest(
        fileInfo, s3HTTPUrl, headers, fileOffset, buffer, bufferLen);
}

std::unique_ptr<HTTPResponse> S3FileSystem::postRequest(common::FileInfo* fileInfo,
    const std::string& url, kuzu::httpfs::HeaderMap /*headerMap*/,
    std::unique_ptr<uint8_t[]>& outputBuffer, uint64_t& outputBufferLen, const uint8_t* inputBuffer,
    uint64_t inputBufferLen, std::string httpParams) const {
    auto& authParams = ku_dynamic_cast<FileInfo*, S3FileInfo*>(fileInfo)->authParams;
    auto parsedS3URL = parseS3URL(url, authParams);
    auto httpURL = parsedS3URL.getHTTPURL(httpParams);
    auto payloadHash = getPayloadHash(inputBuffer, inputBufferLen);
    auto headers = createS3Header(parsedS3URL.path, httpParams, parsedS3URL.host, "s3", "POST",
        authParams, payloadHash, "application/octet-stream");
    return HTTPFileSystem::postRequest(
        fileInfo, httpURL, headers, outputBuffer, outputBufferLen, inputBuffer, inputBufferLen);
}

std::unique_ptr<HTTPResponse> S3FileSystem::putRequest(common::FileInfo* fileInfo,
    const std::string& url, kuzu::httpfs::HeaderMap /*headerMap*/, const uint8_t* inputBuffer,
    uint64_t inputBufferLen, std::string httpParams) const {
    auto& authParams = ku_dynamic_cast<FileInfo*, S3FileInfo*>(fileInfo)->authParams;
    auto parsedS3URL = parseS3URL(url, authParams);
    auto httpURL = parsedS3URL.getHTTPURL(httpParams);
    auto payloadHash = getPayloadHash(inputBuffer, inputBufferLen);
    auto headers = createS3Header(parsedS3URL.path, httpParams, parsedS3URL.host, "s3", "PUT",
        authParams, payloadHash, "application/octet-stream");
    return HTTPFileSystem::putRequest(fileInfo, httpURL, headers, inputBuffer, inputBufferLen);
}

std::string S3FileSystem::getPayloadHash(const uint8_t* buffer, uint64_t bufferLen) {
    if (bufferLen > 0) {
        hash_bytes payloadHashBytes;
        hash_str payloadHashStr;
        sha256(reinterpret_cast<const char*>(buffer), bufferLen, payloadHashBytes);
        hex256(payloadHashBytes, payloadHashStr);
        return std::string((char*)payloadHashStr, sizeof(payloadHashStr));
    } else {
        return "";
    }
}

void S3FileSystem::flushBuffer(
    S3FileInfo* fileInfo, std::shared_ptr<S3WriteBuffer> bufferToFlush) const {
    if (bufferToFlush->numBytesWritten == 0) {
        return;
    }
    auto uploading = bufferToFlush->uploading.load();
    if (uploading) {
        return;
    }
    bool can_upload = bufferToFlush->uploading.compare_exchange_strong(uploading, true);
    if (!can_upload) {
        return;
    }
    fileInfo->rethrowIOError();
    {
        std::unique_lock<std::mutex> lck(fileInfo->writeBuffersLock);
        fileInfo->writeBuffers.erase(bufferToFlush->partID);
    }
    {
        std::unique_lock<std::mutex> lck(fileInfo->uploadsInProgressLock);
        fileInfo->uploadsInProgress++;
    }
    std::thread uploadThread(uploadBuffer, fileInfo, bufferToFlush);
    uploadThread.detach();
}

void S3FileSystem::uploadBuffer(
    S3FileInfo* fileInfo, std::shared_ptr<S3WriteBuffer> bufferToUpload) {
    auto s3FileSystem = ku_dynamic_cast<FileSystem*, S3FileSystem*>(fileInfo->fileSystem);
    std::string queryParam =
        "partNumber=" + std::to_string(bufferToUpload->partID + 1) + "&" +
        "uploadId=" + S3FileSystem::encodeURL(fileInfo->multipartUploadID, true);
    std::unique_ptr<HTTPResponse> res;
    case_insensitive_map_t<std::string>::iterator etagIter;
    try {
        res = s3FileSystem->putRequest(fileInfo, fileInfo->path, {} /* headerMap */,
            bufferToUpload->getData(), bufferToUpload->numBytesWritten, queryParam);
        if (res->code != 200) {
            throw IOException(stringFormat("Unable to connect to URL {} {} (HTTP code {})",
                res->url, res->error, std::to_string(res->code)));
        }
        etagIter = res->headers.find("ETag");
        if (etagIter == res->headers.end()) {
            throw IOException("Unexpected response when uploading part to S3");
        }
    } catch (IOException& ex) {
        // Ensure only one thread sets the exception
        bool hasException = false;
        auto exchanged = fileInfo->uploaderHasException.compare_exchange_strong(hasException, true);
        if (exchanged) {
            fileInfo->uploadException = std::current_exception();
        }
        {
            std::unique_lock<std::mutex> lck(fileInfo->uploadsInProgressLock);
            fileInfo->uploadsInProgress--;
        }
        fileInfo->uploadsInProgressCV.notify_one();

        return;
    }
    {
        std::unique_lock<std::mutex> lck(fileInfo->partEtagsLock);
        fileInfo->partEtags.emplace(bufferToUpload->partID, etagIter->second);
    }
    fileInfo->numPartsUploaded++;
    bufferToUpload.reset();
    {
        std::unique_lock<std::mutex> lck(s3FileSystem->bufferInfoLock);
        s3FileSystem->numUsedBuffers--;
    }
    s3FileSystem->bufferInfoCV.notify_one();
    {
        std::unique_lock<std::mutex> lck(fileInfo->uploadsInProgressLock);
        fileInfo->uploadsInProgress--;
    }
    fileInfo->uploadsInProgressCV.notify_one();
}

std::string S3FileSystem::getUploadID(const std::string& response) {
    auto openTagPos = response.find(uploadIDOpenTag, 0);
    auto closeTagPos = response.find(uploadIDCloseTag, openTagPos);
    if (openTagPos == std::string::npos || closeTagPos == std::string::npos) {
        throw common::IOException("Unexpected response while initializing S3 multipart upload.");
    }
    // Extract the uploadID which has the format <UploadId>uploadID</UploadId>.
    openTagPos += strlen(uploadIDOpenTag);
    return response.substr(openTagPos, closeTagPos - openTagPos);
}

std::string AWSListObjectV2::request(
    std::string& path, S3AuthParams& authParams, std::string& continuationToken) {
    auto parsedURL = S3FileSystem::parseS3URL(path, authParams);
    std::string requestPath;
    if (authParams.urlStyle == "path") {
        requestPath = "/" + parsedURL.bucket + "/";
    } else {
        requestPath = "/";
    }
    auto prefix = parsedURL.path.substr(1);
    if (authParams.urlStyle == "path") {
        prefix = prefix.substr(parsedURL.bucket.length() + 1);
    }
    std::string requestParams = "";
    if (!continuationToken.empty()) {
        requestParams += "continuation-token=" +
                         S3FileSystem::encodeURL(continuationToken, true /* encodeSlash */);
        requestParams += "&";
    }
    requestParams += "encoding-type=url&list-type=2";
    requestParams += "&prefix=" + S3FileSystem::encodeURL(prefix, true);
    std::string listObjectV2URL = requestPath + "?" + requestParams;
    auto headerMap = S3FileSystem::createS3Header(requestPath, requestParams, parsedURL.host, "s3",
        "GET", authParams, "" /* payloadHash */, "" /* contentType */);
    auto headers = HTTPFileSystem::getHTTPHeaders(headerMap);

    auto client = S3FileSystem::getClient(
        parsedURL.httpProto + parsedURL.host); // Get requests use fresh connection
    std::stringstream response;
    auto res = client->Get(
        listObjectV2URL.c_str(), *headers,
        [&](const httplib::Response& response) {
            if (response.status >= 400) {
                throw IOException{common::stringFormat(
                    "HTTP GET error on '{}' (HTTP {})", listObjectV2URL, response.status)};
            }
            return true;
        },
        [&](const char* data, size_t data_length) {
            response << std::string(data, data_length);
            return true;
        });
    if (res.error() != httplib::Error::Success) {
        throw IOException(
            to_string(res.error()) + " error for HTTP GET to '" + listObjectV2URL + "'");
    }

    return response.str();
}

void AWSListObjectV2::parseKey(std::string& awsResponse, std::vector<std::string>& result) {
    uint64_t curPos = 0;
    while (true) {
        auto nextOpenTagPos = awsResponse.find(OPEN_KEY_TAG, curPos);
        if (nextOpenTagPos == std::string::npos) {
            break;
        } else {
            auto nextCloseTagPos =
                awsResponse.find(CLOSE_KEY_TAG, nextOpenTagPos + strlen(OPEN_KEY_TAG));
            if (nextCloseTagPos == std::string::npos) {
                throw RuntimeException("Failed to parse S3 result.");
            }
            auto parsedPath = S3FileSystem::decodeURL(
                awsResponse.substr(nextOpenTagPos + 5, nextCloseTagPos - nextOpenTagPos - 5));
            if (parsedPath.back() != '/') {
                result.push_back(parsedPath);
            }
            curPos = nextCloseTagPos + strlen(CLOSE_KEY_TAG);
        }
    }
}

std::string AWSListObjectV2::parseContinuationToken(std::string& awsResponse) {
    auto openTagPos = awsResponse.find(OPEN_CONTINUATION_TAG);
    if (openTagPos == std::string::npos) {
        return "";
    } else {
        auto closeTagPos =
            awsResponse.find(CLOSE_CONTINUATION_TAG, openTagPos + strlen(OPEN_CONTINUATION_TAG));
        if (closeTagPos == std::string::npos) {
            throw RuntimeException("Failed to parse S3 result.");
        }
        return awsResponse.substr(openTagPos + strlen(OPEN_CONTINUATION_TAG),
            closeTagPos - openTagPos - strlen(OPEN_CONTINUATION_TAG));
    }
}

std::vector<std::string> AWSListObjectV2::parseCommonPrefix(std::string& awsResponse) {
    std::vector<std::string> s3Prefixes;
    uint64_t curPos = 0;
    while (true) {
        curPos = awsResponse.find(OPEN_COMMON_PREFIX_TAG, curPos);
        if (curPos == std::string::npos) {
            break;
        }
        auto nextOpenTagPos = awsResponse.find(OPEN_PREFIX_TAG, curPos);
        if (nextOpenTagPos == std::string::npos) {
            throw RuntimeException("Parsing error while parsing s3 listobject result.");
        } else {
            auto nextCloseTagPos =
                awsResponse.find(CLOSE_PREFIX_TAG, nextOpenTagPos + strlen(OPEN_PREFIX_TAG));
            if (nextCloseTagPos == std::string::npos) {
                throw RuntimeException("Failed to parse S3 result.");
            }
            auto parsedPath = awsResponse.substr(nextOpenTagPos + strlen(OPEN_PREFIX_TAG),
                nextCloseTagPos - nextOpenTagPos - strlen(OPEN_PREFIX_TAG));
            s3Prefixes.push_back(parsedPath);
            curPos = nextCloseTagPos + strlen(CLOSE_KEY_TAG);
        }
    }
    return s3Prefixes;
}

} // namespace httpfs
} // namespace kuzu
