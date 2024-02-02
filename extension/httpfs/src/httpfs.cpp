#include "httpfs.h"

#include "common/cast.h"
#include "common/exception/io.h"

namespace kuzu {
namespace httpfs {

using namespace kuzu::common;

HTTPResponse::HTTPResponse(httplib::Response& res, const std::string& url)
    : code{res.status}, error{res.reason}, url{url}, body{res.body} {
    for (auto& [name, value] : res.headers) {
        headers[name] = value;
    }
}

HTTPFileInfo::HTTPFileInfo(std::string path, FileSystem* fileSystem, int flags)
    : FileInfo{std::move(path), fileSystem}, flags{flags}, length{0},
      availableBuffer{0}, bufferIdx{0}, fileOffset{0}, bufferStartPos{0}, bufferEndPos{0} {}

void HTTPFileInfo::initialize() {
    initializeClient();
    auto hfs = ku_dynamic_cast<const FileSystem*, const HTTPFileSystem*>(fileSystem);
    auto res = hfs->headRequest(ku_dynamic_cast<HTTPFileInfo*, FileInfo*>(this), path, {});
    std::string rangeLength;
    if (res->code != 200) {
        auto accessMode = flags & O_ACCMODE;
        if ((accessMode & O_WRONLY) && res->code == 404) {
            if (!(flags & O_CREAT)) {
                throw IOException(stringFormat("Unable to open URL: \"{}\" for writing: file does "
                                               "not exist and CREATE flag is not set",
                    path));
            }
            length = 0;
            return;
        } else if ((accessMode & O_RDONLY) && res->code != 404) {
            // HEAD request fail, use Range request for another try (read only one byte).
            auto rangeRequest =
                hfs->getRangeRequest(this, this->path, {}, 0, nullptr /* buffer */, 2);
            if (rangeRequest->code != 206) {
                // LCOV_EXCL_START
                throw IOException(stringFormat(
                    "Unable to connect to URL \"{}\": {} ({})", this->path, res->code, res->error));
                // LCOV_EXCL_STOP
            }
            auto rangeFound = rangeRequest->headers["Content-Range"].find("/");

            if (rangeFound == std::string::npos ||
                rangeRequest->headers["Content-Range"].size() < rangeFound + 1) {
                // LCOV_EXCL_START
                throw IOException(stringFormat("Unknown Content-Range Header \"The value of "
                                               "Content-Range Header\":  ({})",
                    rangeRequest->headers["Content-Range"]));
                // LCOV_EXCL_STOP
            }

            rangeLength = rangeRequest->headers["Content-Range"].substr(rangeFound + 1);
            if (rangeLength == "*") {
                // LCOV_EXCL_START
                throw IOException(
                    stringFormat("Unknown total length of the document \"{}\": {} ({})", this->path,
                        res->code, res->error));
                // LCOV_EXCL_STOP
            }
            res = std::move(rangeRequest);
        } else {
            // LCOV_EXCL_START
            throw IOException(stringFormat("Unable to connect to URL \"{}\": {} ({})", res->url,
                std::to_string(res->code), res->error));
            // LCOV_EXCL_STOP
        }
    }

    // Initialize the read buffer now that we know the file exists
    if ((flags & O_ACCMODE) == O_RDONLY) {
        readBuffer = std::make_unique<uint8_t[]>(READ_BUFFER_LEN);
    }

    if (res->headers.find("Content-Length") == res->headers.end() ||
        res->headers["Content-Length"].empty()) {
        // LCOV_EXCL_START
        throw IOException{"Not able to get Content-length of the given file"};
        // LCOV_EXCL_STOP
    } else {
        try {
            if (res->headers.find("Content-Range") == res->headers.end() ||
                res->headers["Content-Range"].empty()) {
                length = std::stoll(res->headers["Content-Length"]);
            } else {
                length = std::stoll(rangeLength);
            }
        } catch (std::invalid_argument& e) {
            // LCOV_EXCL_START
            throw IOException(stringFormat(
                "Invalid Content-Length header received: {}", res->headers["Content-Length"]));
            // LCOV_EXCL_STOP
        } catch (std::out_of_range& e) {
            // LCOV_EXCL_START
            throw IOException(stringFormat(
                "Invalid Content-Length header received: {}", res->headers["Content-Length"]));
            // LCOV_EXCL_STOP
        }
    }
}

void HTTPFileInfo::initializeClient() {
    auto [host, hostPath] = HTTPFileSystem::parseUrl(path);
    httpClient = HTTPFileSystem::getClient(host.c_str());
}

std::unique_ptr<common::FileInfo> HTTPFileSystem::openFile(const std::string& path, int flags,
    main::ClientContext* /*context*/, common::FileLockType /*lock_type*/) {
    auto httpFileInfo = std::make_unique<HTTPFileInfo>(path, this, flags);
    httpFileInfo->initialize();
    return std::move(httpFileInfo);
}

std::vector<std::string> HTTPFileSystem::glob(
    main::ClientContext* /*context*/, const std::string& path) const {
    // Glob is not supported on HTTPFS, simply return the path itself.
    return {path};
}

bool HTTPFileSystem::canHandleFile(const std::string& path) const {
    return path.rfind("https://", 0) == 0 || path.rfind("http://", 0) == 0;
}

void HTTPFileSystem::readFromFile(
    common::FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto numBytesToRead = numBytes;
    auto bufferOffset = 0;
    if (position >= httpFileInfo->bufferStartPos && position < httpFileInfo->bufferEndPos) {
        httpFileInfo->fileOffset = position;
        httpFileInfo->bufferIdx = position - httpFileInfo->bufferStartPos;
        httpFileInfo->availableBuffer =
            (httpFileInfo->bufferEndPos - httpFileInfo->bufferStartPos) - httpFileInfo->bufferIdx;
    } else {
        httpFileInfo->availableBuffer = 0;
        httpFileInfo->bufferIdx = 0;
        httpFileInfo->fileOffset = position;
    }
    while (numBytesToRead > 0) {
        auto buffer_read_len = std::min<uint64_t>(httpFileInfo->availableBuffer, numBytesToRead);
        if (buffer_read_len > 0) {
            KU_ASSERT(httpFileInfo->bufferStartPos + httpFileInfo->bufferIdx + buffer_read_len <=
                      httpFileInfo->bufferEndPos);
            memcpy((char*)buffer + bufferOffset,
                httpFileInfo->readBuffer.get() + httpFileInfo->bufferIdx, buffer_read_len);

            bufferOffset += buffer_read_len;
            numBytesToRead -= buffer_read_len;

            httpFileInfo->bufferIdx += buffer_read_len;
            httpFileInfo->availableBuffer -= buffer_read_len;
            httpFileInfo->fileOffset += buffer_read_len;
        }

        if (numBytesToRead > 0 && httpFileInfo->availableBuffer == 0) {
            auto newBufferAvailableSize = std::min<uint64_t>(
                httpFileInfo->READ_BUFFER_LEN, httpFileInfo->length - httpFileInfo->fileOffset);

            // Bypass buffer if we read more than buffer size.
            if (numBytesToRead > newBufferAvailableSize) {
                getRangeRequest(httpFileInfo, httpFileInfo->path, {}, position + bufferOffset,
                    (char*)buffer + bufferOffset, numBytesToRead);
                httpFileInfo->availableBuffer = 0;
                httpFileInfo->bufferIdx = 0;
                httpFileInfo->fileOffset += numBytesToRead;
                break;
            } else {
                getRangeRequest(httpFileInfo, httpFileInfo->path, {}, httpFileInfo->fileOffset,
                    (char*)httpFileInfo->readBuffer.get(), newBufferAvailableSize);
                httpFileInfo->availableBuffer = newBufferAvailableSize;
                httpFileInfo->bufferIdx = 0;
                httpFileInfo->bufferStartPos = httpFileInfo->fileOffset;
                httpFileInfo->bufferEndPos = httpFileInfo->bufferStartPos + newBufferAvailableSize;
            }
        }
    }
}

int64_t HTTPFileSystem::readFile(common::FileInfo* fileInfo, void* buf, size_t numBytes) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto maxNumBytesToRead = httpFileInfo->length - httpFileInfo->fileOffset;
    numBytes = std::min<uint64_t>(maxNumBytesToRead, numBytes);
    if (httpFileInfo->fileOffset > httpFileInfo->getFileSize()) {
        return 0;
    }
    readFromFile(fileInfo, buf, numBytes, httpFileInfo->fileOffset);
    return numBytes;
}

int64_t HTTPFileSystem::seek(common::FileInfo* fileInfo, uint64_t offset, int /*whence*/) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    httpFileInfo->fileOffset = offset;
    return offset;
}

uint64_t HTTPFileSystem::getFileSize(common::FileInfo* fileInfo) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    return httpFileInfo->length;
}

std::unique_ptr<httplib::Client> HTTPFileSystem::getClient(const std::string& host) {
    auto client = std::make_unique<httplib::Client>(host);
    client->set_follow_location(true);
    client->set_keep_alive(HTTPParams::DEFAULT_KEEP_ALIVE);
    // TODO(Ziyi): This option is needed for https connection
    // client->enable_server_certificate_verification(false);
    client->set_write_timeout(HTTPParams::DEFAULT_TIMEOUT);
    client->set_read_timeout(HTTPParams::DEFAULT_TIMEOUT);
    client->set_connection_timeout(HTTPParams::DEFAULT_TIMEOUT);
    client->set_decompress(false);
    return client;
}

std::unique_ptr<httplib::Headers> HTTPFileSystem::getHTTPHeaders(HeaderMap& headerMap) {
    auto headers = std::make_unique<httplib::Headers>();
    for (auto& entry : headerMap) {
        headers->insert(entry);
    }
    return headers;
}

std::pair<std::string, std::string> HTTPFileSystem::parseUrl(const std::string& url) {
    if (url.rfind("http://", 0) != 0 && url.rfind("https://", 0) != 0) {
        throw IOException("URL needs to start with http:// or https://");
    }
    auto hostPathPos = url.find('/', 8);
    // LCOV_EXCL_START
    if (hostPathPos == std::string::npos) {
        throw IOException("URL needs to contain a '/' after the host");
    }
    // LCOV_EXCL_STOP
    auto host = url.substr(0, hostPathPos);
    auto hostPath = url.substr(hostPathPos);
    // LCOV_EXCEL_START
    if (hostPath.empty()) {
        throw IOException("URL needs to contain a path");
    }
    // LCOV_EXCEL_STOP
    return {host, hostPath};
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::runRequestWithRetry(
    const std::function<httplib::Result(void)>& request, const std::string& url, std::string method,
    const std::function<void(void)>& retry) {
    uint64_t tries = 0;
    while (true) {
        std::exception_ptr exception = nullptr;
        httplib::Error err;
        httplib::Response response;
        int status;

        try {
            auto res = request();
            err = res.error();
            if (err == httplib::Error::Success) {
                status = res->status;
                response = res.value();
            }
        } catch (IOException& e) { exception = std::current_exception(); }

        if (err == httplib::Error::Success) {
            switch (status) {
            case 408: // Request Timeout
            case 418: // Server is pretending to be a teapot
            case 429: // Rate limiter hit
            case 503: // Server has error
            case 504: // Server has error
                break;
            default:
                return std::make_unique<HTTPResponse>(response, url);
            }
        }

        tries += 1;

        if (tries <= HTTPParams::DEFAULT_RETRIES) {
            if (tries > 1) {
                auto sleepTime = (uint64_t)((float)HTTPParams::DEFAULT_RETRY_WAIT_MS *
                                            pow(HTTPParams::DEFAULT_RETRY_BACKOFF, tries - 2));
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));
            }
            if (retry) {
                retry();
            }
        } else {
            if (exception) {
                std::rethrow_exception(exception);
            } else if (err == httplib::Error::Success) {
                // LCOV_EXCL_START
                throw IOException(stringFormat(
                    "Request returned HTTP {} for HTTP {} to '{}'", status, method, url));
                // LCOV_EXCL_STOP
            } else {
                // LCOV_EXCL_START
                throw IOException(
                    stringFormat("{} error for HTTP {} to '{}'", to_string(err), method, url));
                // LCOV_EXCL_STOP
            }
        }
    }
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::headRequest(
    FileInfo* fileInfo, const std::string& url, HeaderMap headerMap) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto parsedURL = parseUrl(url);
    auto host = parsedURL.first;
    auto hostPath = parsedURL.second;
    auto headers = getHTTPHeaders(headerMap);

    std::function<httplib::Result(void)> request(
        [&]() { return httpFileInfo->httpClient->Head(hostPath.c_str(), *headers); });

    std::function<void(void)> retry([&]() { httpFileInfo->httpClient = getClient(host); });

    return runRequestWithRetry(request, url, "HEAD", retry);
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::getRangeRequest(FileInfo* fileInfo,
    const std::string& url, HeaderMap headerMap, uint64_t fileOffset, char* buffer,
    uint64_t bufferLen) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto parsedURL = parseUrl(url);
    auto host = parsedURL.first;
    auto hostPath = parsedURL.second;
    auto headers = getHTTPHeaders(headerMap);

    headers->insert(std::make_pair(
        "Range", stringFormat("bytes={}-{}", fileOffset, fileOffset + bufferLen - 1)));

    uint64_t bufferOffset = 0;

    std::function<httplib::Result(void)> request([&]() {
        return httpFileInfo->httpClient->Get(
            hostPath.c_str(), *headers,
            [&](const httplib::Response& response) {
                if (response.status >= 400) {
                    // LCOV_EXCL_START
                    auto error =
                        stringFormat("HTTP GET error on '{}' (HTTP {})", url, response.status);
                    if (response.status == 416) {
                        error += "Try confirm the server supports range requests.";
                    }
                    throw IOException(error);
                    // LCOV_EXCL_STOP
                }
                if (response.status < 300) {
                    bufferOffset = 0;
                    if (response.has_header("Content-Length")) {
                        auto contentLen = stoll(response.get_header_value("Content-Length", 0));
                        if ((uint64_t)contentLen != bufferLen) {
                            // LCOV_EXCL_START
                            throw IOException(common::stringFormat(
                                "Server returned: {}, HTTP GET error: Content-Length from server "
                                "mismatches requested "
                                "range, server may not support range requests.",
                                response.status));
                            // LCOV_EXCL_STOP
                        }
                    }
                }
                return true;
            },
            [&](const char* data, size_t dataLen) {
                if (buffer != nullptr) {
                    if (dataLen + bufferOffset > bufferLen) {
                        // LCOV_EXCL_START
                        // To avoid corruption of memory, we bail out.
                        throw IOException("Server sent back more data than expected.");
                        // LCOV_EXCL_STOP
                    }
                    memcpy(buffer + bufferOffset, data, dataLen);
                    bufferOffset += dataLen;
                }
                return true;
            });
    });
    std::function<void(void)> retryFunc([&]() { httpFileInfo->httpClient = getClient(host); });
    return runRequestWithRetry(request, url, "GET Range", retryFunc);
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::postRequest(common::FileInfo* fileInfo,
    const std::string& url, HeaderMap headerMap, std::unique_ptr<uint8_t[]>& outputBuffer,
    uint64_t& outputBufferLen, const uint8_t* inputBuffer, uint64_t inputBufferLen,
    std::string /*params*/) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto parsedURL = parseUrl(url);
    auto host = parsedURL.first;
    auto hostPath = parsedURL.second;
    auto headers = getHTTPHeaders(headerMap);
    uint64_t outputBufferPos = 0;

    std::function<httplib::Result(void)> request([&]() {
        auto client = httpFileInfo->httpClient.get();
        httplib::Request req;
        req.method = "POST";
        req.path = hostPath;
        req.headers = *headers;
        req.headers.emplace("Content-Type", "application/octet-stream");
        req.content_receiver = [&](const char* data, size_t dataLen, uint64_t /*offset*/,
                                   uint64_t /*totalLen*/) {
            if (outputBufferPos + dataLen > outputBufferLen) {
                auto newBufferSize =
                    std::max<uint64_t>(outputBufferPos + dataLen, outputBufferLen * 2);
                auto newBuffer = std::make_unique<uint8_t[]>(newBufferSize);
                memcpy(newBuffer.get(), outputBuffer.get(), outputBufferLen);
                outputBuffer = std::move(newBuffer);
                outputBufferLen = newBufferSize;
            }
            memcpy(outputBuffer.get() + outputBufferPos, data, dataLen);
            outputBufferPos += dataLen;
            return true;
        };
        req.body.assign(reinterpret_cast<const char*>(inputBuffer), inputBufferLen);
        return client->send(req);
    });
    return runRequestWithRetry(request, url, "POST");
}

std::unique_ptr<HTTPResponse> HTTPFileSystem::putRequest(common::FileInfo* fileInfo,
    const std::string& url, kuzu::httpfs::HeaderMap headerMap, const uint8_t* inputBuffer,
    uint64_t inputBufferLen, std::string /*params*/) const {
    auto httpFileInfo = ku_dynamic_cast<FileInfo*, HTTPFileInfo*>(fileInfo);
    auto parsedURL = parseUrl(url);
    auto host = parsedURL.first;
    auto hostPath = parsedURL.second;
    auto headers = getHTTPHeaders(headerMap);
    std::function<httplib::Result(void)> request([&]() {
        auto client = httpFileInfo->httpClient.get();
        return client->Put(hostPath.c_str(), *headers, reinterpret_cast<const char*>(inputBuffer),
            inputBufferLen, "application/octet-stream");
    });

    return runRequestWithRetry(request, url, "PUT");
}

} // namespace httpfs
} // namespace kuzu
