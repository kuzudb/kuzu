#include "providers/amazon-bedrock.h"

#include "common/crypto.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/types/timestamp_t.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"

using namespace kuzu::common;

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
httplib::Headers BedrockEmbedding::getHeaders(const nlohmann::json& payload) const {
    static const std::string envVarAWSAccessKey = "AWS_ACCESS_KEY";
    static const std::string envVarAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY";
    auto envAWSAccessKey = main::ClientContext::getEnvVariable(envVarAWSAccessKey);
    auto envAWSSecretAccessKey = main::ClientContext::getEnvVariable(envVarAWSSecretAccessKey);
    if (envAWSAccessKey.empty() || envAWSSecretAccessKey.empty()) {
        std::string errMsg = "The following key(s) could not be read from the environment:\n";
        if (envAWSAccessKey.empty()) {
            errMsg += envVarAWSAccessKey + '\n';
        }
        if (envAWSSecretAccessKey.empty()) {
            errMsg += envVarAWSSecretAccessKey + '\n';
        }
        throw(RuntimeException(errMsg));
    }
    std::string service = "bedrock";
    // TODO(Tanvir): Hardcoded for now, needs to change when a configuration scheme is
    // supported
    std::string region = "us-east-1";
    std::string host = "bedrock-runtime.us-east-1.amazonaws.com";

    auto timestamp = Timestamp::getCurrentTimestamp();
    auto dateHeader = Timestamp::getDateHeader(timestamp);
    auto datetimeHeader = Timestamp::getDateTimeHeader(timestamp);

    std::string canonicalUri = "/model/amazon.titan-embed-text-v1/invoke";
    std::string canonicalQueryString = "";

    httplib::Headers headers{{"host", host}, {"x-amz-date", datetimeHeader}};
    std::string canonicalHeaders;
    std::string signedHeaders;
    for (const auto& header : headers) {
        canonicalHeaders += header.first + ":" + header.second + "\n";
        if (!signedHeaders.empty()) {
            signedHeaders += ";";
        }
        signedHeaders += header.first;
    }

    std::string payloadStr = payload.dump();
    hash_bytes payloadHashBytes;
    hash_str payloadHashHex;
    CryptoUtils::sha256(payloadStr.c_str(), payloadStr.size(), payloadHashBytes);
    CryptoUtils::hex256(payloadHashBytes, payloadHashHex);
    std::ostringstream canonicalRequest;
    canonicalRequest << "POST\n"
                     << canonicalUri << "\n"
                     << canonicalQueryString << "\n"
                     << canonicalHeaders << "\n"
                     << signedHeaders << "\n"
                     << std::string(reinterpret_cast<char*>(payloadHashHex), sizeof(hash_str));
    std::string canonicalRequestStr = canonicalRequest.str();

    hash_bytes canonicalRequestHashBytes;
    hash_str canonicalRequestHashHex;
    CryptoUtils::sha256(canonicalRequestStr.c_str(), canonicalRequestStr.size(),
        canonicalRequestHashBytes);
    CryptoUtils::hex256(canonicalRequestHashBytes, canonicalRequestHashHex);
    std::string algorithm = "AWS4-HMAC-SHA256";
    std::string credentialScope =
        std::string(dateHeader) + "/" + region + "/" + service + "/" + "aws4_request";
    std::ostringstream stringToSign;
    stringToSign << algorithm << "\n"
                 << datetimeHeader << "\n"
                 << credentialScope << "\n"
                 << std::string(reinterpret_cast<char*>(canonicalRequestHashHex), sizeof(hash_str));
    std::string stringToSignStr = stringToSign.str();

    hash_bytes kDate, kRegion, kService, kSigning;
    std::string kSecret = "AWS4" + envAWSSecretAccessKey;
    CryptoUtils::hmac256(dateHeader, kSecret.c_str(), kSecret.size(), kDate);
    CryptoUtils::hmac256(region, kDate, kRegion);
    CryptoUtils::hmac256(service, kRegion, kService);
    CryptoUtils::hmac256("aws4_request", kService, kSigning);
    hash_bytes signatureBytes;
    hash_str signatureHex;
    CryptoUtils::hmac256(stringToSignStr, kSigning, signatureBytes);
    CryptoUtils::hex256(signatureBytes, signatureHex);
    std::ostringstream authorizationHeader;
    authorizationHeader << algorithm << " " << "Credential=" << envAWSAccessKey << "/"
                        << credentialScope << ", " << "SignedHeaders=" << signedHeaders << ", "
                        << "Signature="
                        << std::string(reinterpret_cast<const char*>(signatureHex),
                               sizeof(hash_str));
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
        throw(BinderException("Invalid Model: " + model));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
