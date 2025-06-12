#include "providers/amazon-bedrock.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/types/timestamp_t.h"
#include "crypto.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace llm_extension {

EmbeddingProvider& BedrockEmbedding::getInstance() {
    static BedrockEmbedding instance;
    return instance;
}

std::string BedrockEmbedding::getClient() const {
    return "https://bedrock-runtime." + region.value_or(defaultRegion) + ".amazonaws.com";
}

std::string BedrockEmbedding::getPath(const std::string& /*model*/) const {
    return "/model/amazon.titan-embed-text-v1/invoke";
}

// AWS requests require an authorization signature in the header. This is part
// of a scheme to validate the request. The body is used to create this
// signature. This is one of the reasons the same header cannot be used across
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
        throw(RuntimeException(errMsg + std::string(referenceKuzuDocs)));
    }
    std::string service = "bedrock";
    std::string region = this->region.value_or(defaultRegion);
    std::string host = "bedrock-runtime." + region + ".amazonaws.com";

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

    // For crypto related functionality
    using namespace httpfs_extension;
    std::string payloadStr = payload.dump();
    hash_bytes payloadHashBytes;
    hash_str payloadHashHex;
    sha256(payloadStr.c_str(), payloadStr.size(), payloadHashBytes);
    hex256(payloadHashBytes, payloadHashHex);
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
    sha256(canonicalRequestStr.c_str(), canonicalRequestStr.size(), canonicalRequestHashBytes);
    hex256(canonicalRequestHashBytes, canonicalRequestHashHex);
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
    hmac256(dateHeader, kSecret.c_str(), kSecret.size(), kDate);
    hmac256(region, kDate, kRegion);
    hmac256(service, kRegion, kService);
    hmac256("aws4_request", kService, kSigning);
    hash_bytes signatureBytes;
    hash_str signatureHex;
    hmac256(stringToSignStr, kSigning, signatureBytes);
    hex256(signatureBytes, signatureHex);
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

void BedrockEmbedding::checkModel(const std::string& model) const {
    static const std::unordered_set<std::string> validModels = {"amazon.titan-embed-text-v1"};
    if (validModels.contains(model)) {
        return;
    }
    throw(BinderException("Invalid Model: " + model));
}

void BedrockEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& region) {
    if (dimensions.has_value()) {
        throw(BinderException("Bedrock does not support the dimensions argument: " +
                              std::to_string(dimensions.value()) + '\n' +
                              std::string(referenceKuzuDocs)));
    }
    // Reset to default
    if (!region.has_value()) {
        this->region = std::nullopt;
        return;
    }
    static const std::unordered_set<std::string> validRegions = {"us-east-1", "us-west-2",
        "ap-southeast-1", "ap-northeast-1", "eu-central-1"};
    if (!validRegions.contains(region.value())) {
        throw(BinderException(
            "Invalid Region: " + region.value() + '\n' + std::string(referenceKuzuDocs)));
    }
    this->region = region;
}

uint64_t BedrockEmbedding::getEmbeddingDimension(const std::string& model) const {
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {
        {"amazon.titan-embed-text-v1", 1024}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(BinderException("Invalid Model: " + model + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
