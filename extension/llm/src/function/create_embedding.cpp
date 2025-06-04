#include <cstdint>
#include <cstdlib>
#include <unordered_map>
#include "common/assert.h"
#include "common/exception/binder.h"
#include "common/exception/connection.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "function/function.h"
#include "function/llm_functions.h"
#include "function/scalar_function.h"
#include "crypto.h"
#include "json.hpp"
#include "httplib.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace llm_extension {

static std::string getClient(const std::string& provider)
{
    static const std::unordered_map<std::string, std::string> providerClientMap = 
    {
        {"open-ai", "https://api.openai.com"},
        {"voyage-ai", "https://api.voyageai.com"},
        {"google-vertex", "https://aiplatform.googleapis.com"},
        {"google-gemini", "https://generativelanguage.googleapis.com"},
        // TODO: Region here is hardcoded... when configuration is supported
        // this needs to change
        {"amazon-bedrock", "https://bedrock-runtime.us-east-1.amazonaws.com"},
        {"ollama", "http://localhost:11434"}
    };

    auto clientIter = providerClientMap.find(provider);
    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_ASSERT(clientIter != providerClientMap.end());
    return clientIter->second;
}

std::string getDateHeader(const timestamp_t& timestamp) {
    auto date = Timestamp::getDate(timestamp);
    int32_t year = 0, month = 0, day = 0;
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
    int32_t hours = 0, minutes = 0, seconds = 0, micros = 0;
    Time::convert(time, hours, minutes, seconds, micros);
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

std::string encodeURL(const std::string& input, bool encodeSlash) {
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


static httplib::Headers createBedrockHeaders( const std::string& url, const std::string& query, const std::string& host, const std::string& region, const std::string& service, const std::string& method, std::string payloadHash, std::string contentType = "application/json") {
    httplib::Headers headers;
    headers.insert({"Host", host});

    const char* accessKey = std::getenv("AWS_ACCESS_KEY_ID");
    const char* secretKey = std::getenv("AWS_SECRET_ACCESS_KEY");

    if (!accessKey || !secretKey) 
    {
        throw RuntimeException("Missing AWS credentials in environment variables.");
    }

    if (payloadHash.empty()) {
        static constexpr char NULL_PAYLOAD_HASH[] =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        payloadHash = NULL_PAYLOAD_HASH;
    }

    const auto timestamp = common::Timestamp::getCurrentTimestamp();
    const auto dateHeader = getDateHeader(timestamp);
    const auto datetimeHeader = getDateTimeHeader(timestamp);

    headers.insert({"x-amz-date", datetimeHeader});
    headers.insert({"x-amz-content-sha256", payloadHash});
    headers.insert({"content-type", "application/json"});

    std::string signedHeaders = "content-type;host;x-amz-content-sha256;x-amz-date";

    std::string canonicalHeaders;
    canonicalHeaders +=
        "content-type:" + contentType + "\n"
        "host:" + host + "\n"
        "x-amz-content-sha256:" + payloadHash + "\n"
        "x-amz-date:" + datetimeHeader + "\n";

    std::string canonicalRequest =
        method + "\n" + encodeURL(url, false) + "\n" + query + "\n" +
        canonicalHeaders + "\n" +
        signedHeaders + "\n" +
        payloadHash;

    httpfs_extension::hash_bytes canonicalRequestHash;
    httpfs_extension::hash_str canonicalRequestHashStr;
    httpfs_extension::sha256(canonicalRequest.c_str(), canonicalRequest.length(), canonicalRequestHash);
    httpfs_extension::hex256(canonicalRequestHash, canonicalRequestHashStr);

    std::string credentialScope = dateHeader + "/" + region + "/" + service + "/aws4_request";
    std::string stringToSign = "AWS4-HMAC-SHA256\n" + datetimeHeader + "\n" + credentialScope + "\n" + std::string((char*)canonicalRequestHashStr, sizeof(httpfs_extension::hash_str));

    httpfs_extension::hash_bytes dateKey, regionKey, serviceKey, signingKey, signature;
    httpfs_extension::hash_str signatureStr;

    std::string secret = "AWS4" + std::string(secretKey);
    httpfs_extension::hmac256(dateHeader, secret.c_str(), secret.length(), dateKey);
    httpfs_extension::hmac256(region, dateKey, regionKey);
    httpfs_extension::hmac256(service, regionKey, serviceKey);
    httpfs_extension::hmac256("aws4_request", serviceKey, signingKey);
    httpfs_extension::hmac256(stringToSign, signingKey, signature);
    httpfs_extension::hex256(signature, signatureStr);

    std::string authorization = "AWS4-HMAC-SHA256 Credential=" + std::string(accessKey) + "/" + credentialScope + ", SignedHeaders=" + signedHeaders + ", Signature=" + std::string((char*)signatureStr, sizeof(httpfs_extension::hash_str));

    headers.insert({"Authorization", authorization});

    return headers;
}


static httplib::Headers getHeaders(const std::string& provider)
{
    static const std::unordered_map<std::string, std::string> providerAPIKeyMap = 
    {
            {"open-ai", "OPENAI_API_KEY"},
            {"voyage-ai", "VOYAGE_API_KEY"},
            {"google-vertex", "GOOGLE_VERTEX_ACCESS_KEY"},
    };

    auto apiKeyItr = providerAPIKeyMap.find(provider);
    if (apiKeyItr != providerAPIKeyMap.end())
    {
        auto env_key = std::getenv(apiKeyItr->second.c_str()); //NOLINT thread safety warning
        if (env_key == nullptr) 
        {
            throw(RuntimeException("Could not get key from: "+apiKeyItr->second+'\n'));
        }
        return httplib::Headers{{"Content-Type", "application/json"}, {"Authorization", "Bearer " + std::string(env_key)}};

    }

    else if (provider == "google-gemini" || provider == "ollama")
    {
        return httplib::Headers{{"Content-Type", "applications/json"}};
    }

    else if (provider == "amazon-bedrock")
    {
        //WIP
        //createBedrockHeader(std::string url, std::string query, std::string host, std::string service, std::string method, std::string payloadHash, std::string contentType);
    }

    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_UNREACHABLE;
    return httplib::Headers{};
}

static nlohmann::json getPayload(const std::string& provider, const std::string& model, const std::string& text)
{
    if (provider == "open-ai" || provider == "voyage-ai")
    {
        return nlohmann::json {{"model", model}, {"input", text}};
    }
    else if (provider == "google-gemini")
    {
        return nlohmann::json {{"model", "models/" + model}, {"content", {{"parts", { {{"text", text}}}}}}};
    }

    else if (provider == "google-vertex")
    {
        return nlohmann::json {{"instances",{{{"content", text}}}}};
    }

    else if (provider == "amazon-bedrock")
    {
        return nlohmann::json {{"inputText", text}};
    }

    else if (provider == "ollama")
    {
        return nlohmann::json {{"model", model}, {"prompt", text}};
    }

    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_UNREACHABLE;
    return 0;
}

static std::string getPath(const std::string& provider, const std::string& model)
{
    if (provider == "open-ai" || provider == "voyage-ai")
    {
        return "/v1/embeddings";
    }
    else if (provider == "google-gemini")
    {
        auto env_key = std::getenv("GEMINI_API_KEY"); //NOLINT thread safety warning
        if (env_key == nullptr)
        {
            throw(RuntimeException("Could not get key from: GEMINI_API_KEY\n"));
        }
        return "/v1beta/models/"+ model +":embedContent?key=" + std::string(env_key);
    }

    else if (provider == "google-vertex")
    {
        auto env_project_id = std::getenv("GOOGLE_CLOUD_PROJECT_ID"); //NOLINT thread safety warning
        if (env_project_id == nullptr)
        {
            throw(RuntimeException("Could not get project id from: GOOGLE_CLOUD_PROJECT_ID\n"));
        }

        //TODO: Location is hardcoded, this should be changed
        return "/v1/projects/"+std::string(env_project_id)+"/locations/us-central1/publishers/google/models/"+model+":predict";
    }

    else if (provider == "amazon-bedrock")
    {
        return "/model/amazon.titan-embed-text-v1/invoke";
    }

    else if (provider == "ollama")
    {
        return "/api/embeddings";
    }
    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_UNREACHABLE;
    return std::string();

}


static std::vector<float> getEmbedding(const httplib::Result& res, const std::string& provider)
{
    if (provider == "open-ai" || provider == "voyage-ai")
    {
        return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
    }
    else if (provider == "google-gemini")
    {
        return nlohmann::json::parse(res->body)["embedding"]["values"].get<std::vector<float>>();
    }
    else if (provider == "google-vertex")
    {
        return nlohmann::json::parse(res->body)["predictions"][0]["embeddings"]["values"].get<std::vector<float>>();
    }
    else if (provider == "amazon-bedrock" || provider == "ollama")
    {
        return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
    }

    KU_UNREACHABLE;
    return std::vector<float>();

}


static uint64_t getEmbeddingDimensions(const std::string& provider, const std::string& model)
{
    static const std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> providerModelMap = 
    {
            {"open-ai", {{"text-embedding-3-large", 3072}, {"text-embedding-3-small", 1536}, {"text-embedding-ada-002", 1536}}},
            {"voyage-ai", {{"voyage-3-large", 1024}, {"voyage-3.5", 1024}, {"voyage-3.5-lite", 1024}, {"voyage-code-3", 1024}, {"voyage-finance-2", 1024}, {"voyage-law-2", 1024}, {"voyage-code-2", 1536}}},
            {"ollama", {{"nomic-embed-text", 768}, {"all-minilm:l6-v2", 384}}},
            {"amazon-bedrock", {{"amazon.titan-embed-text-v1", 1024}}},
            {"google-vertex", {{"gemini-embedding-001", 3072}, {"text-embedding-005", 768}, {"text-multilingual-embedding-002", 768}}},
            {"google-gemini", {{"gemini-embedding-exp-03-07", 3072}, {"text-embedding-004", 768}, {"embedding-001", 768}}}
    };

    auto providerItr = providerModelMap.find(provider);
    if (providerItr == providerModelMap.end())
    {
        throw(BinderException("Invalid Provider: " + provider));
    }
    auto modelItr = providerItr->second.find(model);
    if (modelItr == providerItr->second.end())
    {
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
    httplib::Headers headers = getHeaders(provider);
    std::string path = getPath(provider, model);


    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {


        auto text = parameters[0]->getValue<ku_string_t>(selectedPos).getAsString();


        nlohmann::json payload = getPayload(provider, model, text);


        auto res = client.Post(path, headers, payload.dump(), "application/json");


        if (!res) {
            throw ConnectionException(
                "Request failed: Could not connect to server\n");
        } else if (res->status != 200) {
            throw ConnectionException("Request failed with status " + std::to_string(res->status) + "\n Body: " + res->body + "\n");
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
    uint64_t embeddingDimensions = getEmbeddingDimensions(StringUtils::getLower(input.arguments[1]->toString()), StringUtils::getLower(input.arguments[2]->toString()));
    return std::make_unique<FunctionBindData>(std::move(types), LogicalType::ARRAY(LogicalType(LogicalTypeID::FLOAT), embeddingDimensions));
}

function_set CreateEmbedding::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING}, LogicalTypeID::ARRAY, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace llm_extension
} // namespace kuzu
