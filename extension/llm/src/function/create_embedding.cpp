#include "common/exception/binder.h"
#include "common/exception/connection.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "function/llm_functions.h"
#include "function/scalar_function.h"
#include "httplib.h"
#include "json.hpp"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace llm_extension {

static std::string getClient(const std::string& provider) {
    static const std::unordered_map<std::string, std::string> providerClientMap = {
        {"open-ai", "https://api.openai.com"}, {"voyage-ai", "https://api.voyageai.com"},
        {"google-vertex", "https://aiplatform.googleapis.com"},
        {"google-gemini", "https://generativelanguage.googleapis.com"},
        // TODO: Region here is hardcoded... when configuration is supported
        // this needs to change
        {"amazon-bedrock", "https://bedrock-runtime.us-east-1.amazonaws.com"},
        {"ollama", "http://localhost:11434"}};

    auto clientIter = providerClientMap.find(provider);
    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_ASSERT(clientIter != providerClientMap.end());
    return clientIter->second;
}

static httplib::Headers getHeaders(const std::string& provider) {
    static const std::unordered_map<std::string, std::string> providerAPIKeyMap = {
        {"open-ai", "OPENAI_API_KEY"},
        {"voyage-ai", "VOYAGE_API_KEY"},
        {"google-vertex", "GOOGLE_VERTEX_ACCESS_KEY"},
    };

    auto apiKeyItr = providerAPIKeyMap.find(provider);
    if (apiKeyItr != providerAPIKeyMap.end()) {
        // NOLINTNEXTLINE thread safety warning
        auto env_key = std::getenv(apiKeyItr->second.c_str());
        if (env_key == nullptr) {
            throw(RuntimeException("Could not get key from: " + apiKeyItr->second + '\n'));
        }
        return httplib::Headers{{"Content-Type", "application/json"},
            {"Authorization", "Bearer " + std::string(env_key)}};

    }

    else if (provider == "google-gemini" || provider == "ollama") {
        return httplib::Headers{{"Content-Type", "applications/json"}};
    }

    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    // AmazonBedrock Uses createBedrockHeaders
    KU_UNREACHABLE;
    return httplib::Headers{};
}

// AWS Signature Helpers Start

std::string Hex(const unsigned char* data, size_t length) {
    std::ostringstream oss;
    for (size_t i = 0; i < length; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)data[i];
    }
    return oss.str();
}

std::vector<unsigned char> HMAC_SHA256_Raw(const std::string& key, const std::string& message) {
    unsigned int len = SHA256_DIGEST_LENGTH;
    std::vector<unsigned char> result(len);
    HMAC(EVP_sha256(), key.c_str(), key.length(),
        reinterpret_cast<const unsigned char*>(message.c_str()), message.length(), result.data(),
        &len);
    return result;
}

std::string SHA256Hash(const std::string& input) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(input.c_str()), input.size(), hash);
    return Hex(hash, SHA256_DIGEST_LENGTH);
}

std::vector<unsigned char> GetSignatureKey(const std::string& key, const std::string& dateStamp,
    const std::string& regionName, const std::string& serviceName) {
    auto kDate = HMAC_SHA256_Raw("AWS4" + key, dateStamp);
    auto kRegion = HMAC_SHA256_Raw(std::string(kDate.begin(), kDate.end()), regionName);
    auto kService = HMAC_SHA256_Raw(std::string(kRegion.begin(), kRegion.end()), serviceName);
    auto kSigning = HMAC_SHA256_Raw(std::string(kService.begin(), kService.end()), "aws4_request");
    return kSigning;
}

// AWS Signature Helpers End

// AWS requests require an authorization signature in the header. This is part of a scheme to
// validate the request. The body is used to create this signature. This is one of the reasons the
// same header cannot be used accross different requests. Refer to
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
static httplib::Headers getAWSBedrockHeader(const nlohmann::json& payload) {
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
        throw(RuntimeException(errMsg));
    }

    std::string service = "bedrock";

    // Hardcoded for now, needs to change when a configuration scheme is supported
    std::string region = "us-east-1";
    std::string host = "bedrock-runtime.us-east-1.amazonaws.com";

    time_t now = time(nullptr);
    tm tm_struct{};

    {
        auto status = gmtime_r(&now, &tm_struct);
        if (status == nullptr) {
            throw(RuntimeException("Failure to convert the specified time to UTC"));
        }
    }

    char dateStamp[9];
    char amzDate[17];
    {
        const char* dateStampPattern = "%Y%m%d";
        const char* amzDatePattern = "%Y%m%dT%H%M%SZ";
        auto status = strftime(dateStamp, sizeof(dateStamp), dateStampPattern, &tm_struct);
        if (status == 0) {
            throw(
                RuntimeException("Unable to format dateStamp with pattern " +
                                 std::string(dateStampPattern) + " (UTC time conversion failed)"));
        }

        status = strftime(amzDate, sizeof(amzDate), amzDatePattern, &tm_struct);
        if (status == 0) {
            throw(RuntimeException("Unable to format amzDate with pattern " +
                                   std::string(amzDatePattern) + " (UTC time conversion failed)"));
        }
    }

    std::string canonicalUri = "/model/amazon.titan-embed-text-v1/invoke";
    std::string canonicalQueryString = "";
    httplib::Headers headers{{"host", host}, {"x-amz-date", amzDate}};
    std::string canonicalHeaders;
    std::string signedHeaders;
    for (const auto& h : headers) {
        canonicalHeaders += h.first + ":" + h.second + "\n";
        if (!signedHeaders.empty())
            signedHeaders += ";";
        signedHeaders += h.first;
    }
    std::string payloadHash = SHA256Hash(payload.dump());
    std::ostringstream canonicalRequest;
    canonicalRequest << "POST\n"
                     << canonicalUri << "\n"
                     << canonicalQueryString << "\n"
                     << canonicalHeaders << "\n"
                     << signedHeaders << "\n"
                     << payloadHash;
    std::string canonicalRequestStr = canonicalRequest.str();

    std::string algorithm = "AWS4-HMAC-SHA256";
    std::string credentialScope =
        std::string(dateStamp) + "/" + region + "/" + service + "/" + "aws4_request";
    std::string canonicalRequestHash = SHA256Hash(canonicalRequestStr);
    std::ostringstream stringToSign;
    stringToSign << algorithm << "\n"
                 << amzDate << "\n"
                 << credentialScope << "\n"
                 << canonicalRequestHash;
    std::string stringToSignStr = stringToSign.str();

    auto signingKey =
        GetSignatureKey(std::string(envAWSSecretAccessKey), dateStamp, region, service);
    unsigned int len = SHA256_DIGEST_LENGTH;
    unsigned char signatureRaw[SHA256_DIGEST_LENGTH];
    HMAC(EVP_sha256(), signingKey.data(), signingKey.size(),
        reinterpret_cast<const unsigned char*>(stringToSignStr.c_str()), stringToSignStr.size(),
        signatureRaw, &len);

    std::string signature = Hex(signatureRaw, len);
    std::ostringstream authorizationHeader;
    authorizationHeader << algorithm << " " << "Credential=" << std::string(envAWSAccessKey) << "/"
                        << credentialScope << ", " << "SignedHeaders=" << signedHeaders << ", "
                        << "Signature=" << signature;

    std::string authHeader = authorizationHeader.str();
    headers.insert({"Authorization", authHeader});
    return headers;
}

static nlohmann::json getPayload(const std::string& provider, const std::string& model,
    const std::string& text) {
    if (provider == "open-ai" || provider == "voyage-ai") {
        return nlohmann::json{{"model", model}, {"input", text}};
    } else if (provider == "google-gemini") {
        return nlohmann::json{{"model", "models/" + model},
            {"content", {{"parts", {{{"text", text}}}}}}};
    }

    else if (provider == "google-vertex") {
        return nlohmann::json{{"instances", {{{"content", text}}}}};
    }

    else if (provider == "amazon-bedrock") {
        return nlohmann::json{{"inputText", text}};
    }

    else if (provider == "ollama") {
        return nlohmann::json{{"model", model}, {"prompt", text}};
    }

    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_UNREACHABLE;
    return 0;
}

static std::string getPath(const std::string& provider, const std::string& model) {
    if (provider == "open-ai" || provider == "voyage-ai") {
        return "/v1/embeddings";
    } else if (provider == "google-gemini") {
        // NOLINTNEXTLINE thread safety warning
        auto env_key = std::getenv("GEMINI_API_KEY");
        if (env_key == nullptr) {
            throw(RuntimeException("Could not get key from: GEMINI_API_KEY\n"));
        }
        return "/v1beta/models/" + model + ":embedContent?key=" + std::string(env_key);
    }

    else if (provider == "google-vertex") {
        // NOLINTNEXTLINE thread safety warning
        auto env_project_id = std::getenv("GOOGLE_CLOUD_PROJECT_ID");
        if (env_project_id == nullptr) {
            throw(RuntimeException("Could not get project id from: GOOGLE_CLOUD_PROJECT_ID\n"));
        }

        // TODO: Location is hardcoded, this should be changed when configuration is supported
        return "/v1/projects/" + std::string(env_project_id) +
               "/locations/us-central1/publishers/google/models/" + model + ":predict";
    }

    else if (provider == "amazon-bedrock") {
        return "/model/amazon.titan-embed-text-v1/invoke";
    }

    else if (provider == "ollama") {
        return "/api/embeddings";
    }
    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_UNREACHABLE;
    return std::string();
}

static std::vector<float> getEmbedding(const httplib::Result& res, const std::string& provider) {
    if (provider == "open-ai" || provider == "voyage-ai") {
        return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
    } else if (provider == "google-gemini") {
        return nlohmann::json::parse(res->body)["embedding"]["values"].get<std::vector<float>>();
    } else if (provider == "google-vertex") {
        return nlohmann::json::parse(res->body)["predictions"][0]["embeddings"]["values"]
            .get<std::vector<float>>();
    } else if (provider == "amazon-bedrock" || provider == "ollama") {
        return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
    }

    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_UNREACHABLE;
    return std::vector<float>();
}

static uint64_t getEmbeddingDimensions(const std::string& provider, const std::string& model) {
    static const std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>>
        providerModelMap = {
            {"open-ai", {{"text-embedding-3-large", 3072}, {"text-embedding-3-small", 1536},
                            {"text-embedding-ada-002", 1536}}},
            {"voyage-ai",
                {{"voyage-3-large", 1024}, {"voyage-3.5", 1024}, {"voyage-3.5-lite", 1024},
                    {"voyage-code-3", 1024}, {"voyage-finance-2", 1024}, {"voyage-law-2", 1024},
                    {"voyage-code-2", 1536}}},
            {"ollama", {{"nomic-embed-text", 768}, {"all-minilm:l6-v2", 384}}},
            {"amazon-bedrock", {{"amazon.titan-embed-text-v1", 1024}}},
            {"google-vertex", {{"gemini-embedding-001", 3072}, {"text-embedding-005", 768},
                                  {"text-multilingual-embedding-002", 768}}},
            {"google-gemini", {{"gemini-embedding-exp-03-07", 3072}, {"text-embedding-004", 768},
                                  {"embedding-001", 768}}}};

    auto providerItr = providerModelMap.find(provider);
    if (providerItr == providerModelMap.end()) {
        throw(BinderException("Invalid Provider: " + provider));
    }
    auto modelItr = providerItr->second.find(model);
    if (modelItr == providerItr->second.end()) {
        throw(BinderException("Invalid Model: " + model));
    }
    return modelItr->second;
}

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/,
    common::ValueVector& result, common::SelectionVector* resultSelVector, void* /*dataPtr*/) {

    auto provider = StringUtils::getLower(parameters[1]->getValue<ku_string_t>(0).getAsString());
    auto model = StringUtils::getLower(parameters[2]->getValue<ku_string_t>(0).getAsString());
    httplib::Client client(getClient(provider));
    client.set_connection_timeout(30, 0);
    client.set_read_timeout(30, 0);
    client.set_write_timeout(30, 0);
    httplib::Headers headers;

    // Amazon Bedrock headers differ per request due to a signature authorization scheme
    bool bedRock = provider == "amazon-bedrock";
    if (!bedRock) {
        headers = getHeaders(provider);
    }
    std::string path = getPath(provider, model);

    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {

        auto text = parameters[0]->getValue<ku_string_t>(selectedPos).getAsString();

        nlohmann::json payload = getPayload(provider, model, text);

        if (bedRock) {
            headers = getAWSBedrockHeader(payload);
        }
        auto res = client.Post(path, headers, payload.dump(), "application/json");

        if (!res) {
            throw ConnectionException("Request failed: Could not connect to server\n");
        } else if (res->status != 200) {
            throw ConnectionException("Request failed with status " + std::to_string(res->status) +
                                      "\n Body: " + res->body + "\n");
        }

        auto embeddingVec = getEmbedding(res, provider);
        auto pos = (*resultSelVector)[selectedPos];
        auto resultEntry = ListVector::addList(&result, embeddingVec.size());
        result.setValue(pos, resultEntry);
        auto resultDataVector = ListVector::getDataVector(&result);
        auto resultPos = resultEntry.offset;
        for (auto i = 0u; i < embeddingVec.size(); i++) {
            resultDataVector->copyFromValue(resultPos++, Value(embeddingVec[i]));
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());
    types.push_back(input.arguments[2]->getDataType().copy());
    uint64_t embeddingDimensions =
        getEmbeddingDimensions(StringUtils::getLower(input.arguments[1]->toString()),
            StringUtils::getLower(input.arguments[2]->toString()));
    return std::make_unique<FunctionBindData>(std::move(types),
        LogicalType::ARRAY(LogicalType(LogicalTypeID::FLOAT), embeddingDimensions));
}

function_set CreateEmbedding::getFunctionSet() {
    function_set functionSet;
    // Prompt, Provider, Model -> Vector Embedding
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::ARRAY, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace llm_extension
} // namespace kuzu
