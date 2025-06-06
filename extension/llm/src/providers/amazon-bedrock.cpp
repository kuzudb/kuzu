#include "providers/amazon-bedrock.h"

#include "common/crypto.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/types/timestamp_t.h"
#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider& BedrockEmbedding::getInstance() {
    static BedrockEmbedding instance;
    return instance;
}
std::string BedrockEmbedding::getClient() const {
    return "https://bedrock-runtime.us-east-1.amazonaws.com";
}
std::string BedrockEmbedding::getPath(const std::string& /*model*/) const {
    return "/model/amazon.titan-embed-text-v1/invoke";
}

// AWS requests require an authorization signature in the header. This is part
// of a scheme to validate the request. The body is used to create this
// signature. This is one of the reasons the same header cannot be used accross
// different requests. Refer to
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html

static std::string getDateHeader(const common::timestamp_t& timestamp) {
    auto date = common::Timestamp::getDate(timestamp);
    int32_t year = 0, month = 0, day = 0;
    common::Date::convert(date, year, month, day);
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
static std::string getDateTimeHeader(const common::timestamp_t& timestamp) {
    auto formatStr = getDateHeader(timestamp);
    auto time = common::Timestamp::getTime(timestamp);
    formatStr += "T";
    int32_t hours = 0, minutes = 0, seconds = 0, micros = 0;
    common::Time::convert(time, hours, minutes, seconds, micros);
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

httplib::Headers BedrockEmbedding::getHeaders(const nlohmann::json& payload) const {
    static const std::string envVarAWSAccessKey = "AWS_ACCESS_KEY";
    static const std::string envVarAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY";
    // NOLINTNEXTLINE Thread Safety Warning
    auto envAWSAccessKey = std::getenv(envVarAWSAccessKey.c_str());
    // NOLINTNEXTLINE Thread Safety Warning
    auto envAWSSecretAccessKey = std::getenv(envVarAWSSecretAccessKey.c_str());
    if (envAWSAccessKey == nullptr || envAWSSecretAccessKey == nullptr) {
        std::string errMsg = "The following key(s) could not be read from the environment:\n";
        if (!envAWSAccessKey) {
            errMsg += envVarAWSAccessKey + '\n';
        }

        if (!envAWSSecretAccessKey) {
            errMsg += envVarAWSSecretAccessKey + '\n';
        }
        throw(kuzu::common::RuntimeException(errMsg));
    }

    std::string service = "bedrock";

    // Hardcoded for now, needs to change when a configuration scheme is
    // supported
    std::string region = "us-east-1";
    std::string host = "bedrock-runtime.us-east-1.amazonaws.com";

    auto timestamp = common::Timestamp::getCurrentTimestamp();
    auto dateHeader = getDateHeader(timestamp);
    auto datetimeHeader = getDateTimeHeader(timestamp);

    std::string canonicalUri = "/model/amazon.titan-embed-text-v1/invoke";
    std::string canonicalQueryString = "";
    httplib::Headers headers{{"host", host}, {"x-amz-date", datetimeHeader}};
    std::string canonicalHeaders;
    std::string signedHeaders;
    for (const auto& h : headers) {
        canonicalHeaders += h.first + ":" + h.second + "\n";
        if (!signedHeaders.empty()) {
            signedHeaders += ";";
        }
        signedHeaders += h.first;
    }

    std::string payloadStr = payload.dump();
    kuzu::common::hash_bytes payloadHashBytes;
    kuzu::common::hash_str payloadHashHex;
    kuzu::common::sha256(payloadStr.c_str(), payloadStr.size(), payloadHashBytes);
    kuzu::common::hex256(payloadHashBytes, payloadHashHex);

    std::ostringstream canonicalRequest;
    canonicalRequest << "POST\n"
                     << canonicalUri << "\n"
                     << canonicalQueryString << "\n"
                     << canonicalHeaders << "\n"
                     << signedHeaders << "\n"
                     << std::string(reinterpret_cast<char*>(payloadHashHex),
                            sizeof(kuzu::common::hash_str));
    std::string canonicalRequestStr = canonicalRequest.str();

    kuzu::common::hash_bytes canonicalRequestHashBytes;
    kuzu::common::hash_str canonicalRequestHashHex;
    kuzu::common::sha256(canonicalRequestStr.c_str(), canonicalRequestStr.size(),
        canonicalRequestHashBytes);
    kuzu::common::hex256(canonicalRequestHashBytes, canonicalRequestHashHex);

    std::string algorithm = "AWS4-HMAC-SHA256";
    std::string credentialScope =
        std::string(dateHeader) + "/" + region + "/" + service + "/" + "aws4_request";

    std::ostringstream stringToSign;
    stringToSign << algorithm << "\n"
                 << datetimeHeader << "\n"
                 << credentialScope << "\n"
                 << std::string(reinterpret_cast<char*>(canonicalRequestHashHex),
                        sizeof(kuzu::common::hash_str));

    std::string stringToSignStr = stringToSign.str();

    kuzu::common::hash_bytes kDate, kRegion, kService, kSigning;
    std::string kSecret = "AWS4" + std::string(envAWSSecretAccessKey);
    kuzu::common::hmac256(dateHeader, kSecret.c_str(), kSecret.size(), kDate);
    kuzu::common::hmac256(region, kDate, kRegion);
    kuzu::common::hmac256(service, kRegion, kService);
    kuzu::common::hmac256("aws4_request", kService, kSigning);

    kuzu::common::hash_bytes signatureBytes;
    kuzu::common::hash_str signatureHex;

    kuzu::common::hmac256(stringToSignStr, kSigning, signatureBytes);
    kuzu::common::hex256(signatureBytes, signatureHex);
    std::ostringstream authorizationHeader;
    authorizationHeader << algorithm << " " << "Credential=" << std::string(envAWSAccessKey) << "/"
                        << credentialScope << ", " << "SignedHeaders=" << signedHeaders << ", "
                        << "Signature="
                        << std::string(reinterpret_cast<const char*>(signatureHex),
                               sizeof(kuzu::common::hash_str));

    headers.insert({"Authorization", authorizationHeader.str()});
    return headers;
}
nlohmann::json BedrockEmbedding::getPayload(const std::string& /*model*/,
    const std::string& text) const {
    return nlohmann::json{{"inputText", text}};
}
std::vector<float> BedrockEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
}
uint64_t BedrockEmbedding::getEmbeddingDimension(const std::string& model) {
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {
        {"amazon.titan-embed-text-v1", 1024}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(common::BinderException("Invalid Model: " + model));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
