#include "common/assert.h"
#include "common/exception/binder.h"
#include "common/exception/connection.h"
#include "common/string_utils.h"
#include "function/llm_functions.h"
#include "function/scalar_function.h"
#include "httplib.h"
#include "json.hpp"
#include "providers/amazon-bedrock.h"
#include "providers/google-gemini.h"
#include "providers/google-vertex.h"
#include "providers/open-ai.h"
#include "providers/ollama.h"
#include "providers/provider.h"
#include "providers/voyage-ai.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace llm_extension {


EmbeddingProvider& getInstance(const std::string& provider)
{
    if (provider == "open-ai")
    {
        return OpenAIEmbedding::getInstance();
    }
    else if (provider == "voyage-ai")
    {
        return VoyageAIEmbedding::getInstance();
    }
    else if (provider == "google-vertex")
    {
        return GoogleVertexEmbedding::getInstance();
    }
    else if (provider == "google-gemini")
    {
        return GoogleGeminiEmbedding::getInstance();
    }
    else if (provider == "amazon-bedrock")
    {
        return BedrockEmbedding::getInstance();
    }
    else if (provider == "ollama")
    {
        return OllamaEmbedding::getInstance();
    }
    throw BinderException("Provider not found: "+provider+"\n");
    KU_UNREACHABLE;
    return OllamaEmbedding::getInstance();
}

//// AWS Signature Helpers Start
//
//static std::string Hex(const unsigned char* data, size_t length) {
//    std::ostringstream oss;
//    for (size_t i = 0; i < length; ++i) {
//        oss << std::hex << std::setw(2) << std::setfill('0') << (int)data[i];
//    }
//    return oss.str();
//}
//
//static std::vector<unsigned char> HMAC_SHA256_Raw(const std::string& key,
//    const std::string& message) {
//    unsigned int len = SHA256_DIGEST_LENGTH;
//    std::vector<unsigned char> result(len);
//    HMAC(EVP_sha256(), key.c_str(), key.length(),
//        reinterpret_cast<const unsigned char*>(message.c_str()), message.length(), result.data(),
//        &len);
//    return result;
//}
//
//static std::string SHA256Hash(const std::string& input) {
//    unsigned char hash[SHA256_DIGEST_LENGTH];
//    SHA256(reinterpret_cast<const unsigned char*>(input.c_str()), input.size(), hash);
//    return Hex(hash, SHA256_DIGEST_LENGTH);
//}
//
//static std::vector<unsigned char> GetSignatureKey(const std::string& key,
//    const std::string& dateStamp, const std::string& regionName, const std::string& serviceName) {
//    auto kDate = HMAC_SHA256_Raw("AWS4" + key, dateStamp);
//    auto kRegion = HMAC_SHA256_Raw(std::string(kDate.begin(), kDate.end()), regionName);
//    auto kService = HMAC_SHA256_Raw(std::string(kRegion.begin(), kRegion.end()), serviceName);
//    auto kSigning = HMAC_SHA256_Raw(std::string(kService.begin(), kService.end()), "aws4_request");
//    return kSigning;
//}
//
//// AWS Signature Helpers End
//
//// AWS requests require an authorization signature in the header. This is part of a scheme to
//// validate the request. The body is used to create this signature. This is one of the reasons the
//// same header cannot be used accross different requests. Refer to
//// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
//static httplib::Headers getAWSBedrockHeader(const nlohmann::json& payload) {
//    static const std::string envVarAWSAccessKey = "AWS_ACCESS_KEY";
//    static const std::string envVarAWSSecretAccessKey = "AWS_SECRET_ACCESS_KEY";
//    // NOLINTNEXTLINE Thread Safety Warning
//    auto envAWSAccessKey = std::getenv(envVarAWSAccessKey.c_str());
//    // NOLINTNEXTLINE Thread Safety Warning
//    auto envAWSSecretAccessKey = std::getenv(envVarAWSSecretAccessKey.c_str());
//    if (envAWSAccessKey == nullptr || envAWSSecretAccessKey == nullptr) {
//        std::string errMsg = "The following key(s) could not be read from the environment:\n";
//        if (!envAWSAccessKey) {
//            errMsg += envVarAWSAccessKey + '\n';
//        }
//
//        if (!envAWSSecretAccessKey) {
//            errMsg += envVarAWSSecretAccessKey + '\n';
//        }
//        throw(RuntimeException(errMsg));
//    }
//
//    std::string service = "bedrock";
//
//    // Hardcoded for now, needs to change when a configuration scheme is supported
//    std::string region = "us-east-1";
//    std::string host = "bedrock-runtime.us-east-1.amazonaws.com";
//
//    time_t now = time(nullptr);
//    tm tm_struct{};
//
//    {
//        auto status = gmtime_r(&now, &tm_struct);
//        if (status == nullptr) {
//            throw(RuntimeException("Failure to convert the specified time to UTC"));
//        }
//    }
//
//    char dateStamp[9];
//    char amzDate[17];
//    {
//        const char* dateStampPattern = "%Y%m%d";
//        const char* amzDatePattern = "%Y%m%dT%H%M%SZ";
//        auto status = strftime(dateStamp, sizeof(dateStamp), dateStampPattern, &tm_struct);
//        if (status == 0) {
//            throw(
//                RuntimeException("Unable to format dateStamp with pattern " +
//                                 std::string(dateStampPattern) + " (UTC time conversion failed)"));
//        }
//
//        status = strftime(amzDate, sizeof(amzDate), amzDatePattern, &tm_struct);
//        if (status == 0) {
//            throw(RuntimeException("Unable to format amzDate with pattern " +
//                                   std::string(amzDatePattern) + " (UTC time conversion failed)"));
//        }
//    }
//
//    std::string canonicalUri = "/model/amazon.titan-embed-text-v1/invoke";
//    std::string canonicalQueryString = "";
//    httplib::Headers headers{{"host", host}, {"x-amz-date", amzDate}};
//    std::string canonicalHeaders;
//    std::string signedHeaders;
//    for (const auto& h : headers) {
//        canonicalHeaders += h.first + ":" + h.second + "\n";
//        if (!signedHeaders.empty())
//            signedHeaders += ";";
//        signedHeaders += h.first;
//    }
//    std::string payloadHash = SHA256Hash(payload.dump());
//    std::ostringstream canonicalRequest;
//    canonicalRequest << "POST\n"
//                     << canonicalUri << "\n"
//                     << canonicalQueryString << "\n"
//                     << canonicalHeaders << "\n"
//                     << signedHeaders << "\n"
//                     << payloadHash;
//    std::string canonicalRequestStr = canonicalRequest.str();
//
//    std::string algorithm = "AWS4-HMAC-SHA256";
//    std::string credentialScope =
//        std::string(dateStamp) + "/" + region + "/" + service + "/" + "aws4_request";
//    std::string canonicalRequestHash = SHA256Hash(canonicalRequestStr);
//    std::ostringstream stringToSign;
//    stringToSign << algorithm << "\n"
//                 << amzDate << "\n"
//                 << credentialScope << "\n"
//                 << canonicalRequestHash;
//    std::string stringToSignStr = stringToSign.str();
//
//    auto signingKey =
//        GetSignatureKey(std::string(envAWSSecretAccessKey), dateStamp, region, service);
//    unsigned int len = SHA256_DIGEST_LENGTH;
//    unsigned char signatureRaw[SHA256_DIGEST_LENGTH];
//    HMAC(EVP_sha256(), signingKey.data(), signingKey.size(),
//        reinterpret_cast<const unsigned char*>(stringToSignStr.c_str()), stringToSignStr.size(),
//        signatureRaw, &len);
//
//    std::string signature = Hex(signatureRaw, len);
//    std::ostringstream authorizationHeader;
//    authorizationHeader << algorithm << " " << "Credential=" << std::string(envAWSAccessKey) << "/"
//                        << credentialScope << ", " << "SignedHeaders=" << signedHeaders << ", "
//                        << "Signature=" << signature;
//
//    headers.insert({"Authorization", authorizationHeader.str()});
//    return headers;
//}

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/,
    common::ValueVector& result, common::SelectionVector* resultSelVector, void* /*dataPtr*/) {

    auto provider = StringUtils::getLower(parameters[1]->getValue<ku_string_t>(0).getAsString());
    auto model = StringUtils::getLower(parameters[2]->getValue<ku_string_t>(0).getAsString());
    auto& embeddingProvider = getInstance(provider);
    httplib::Client client(embeddingProvider.getClient());
    client.set_connection_timeout(30, 0);
    client.set_read_timeout(30, 0);
    client.set_write_timeout(30, 0);
    httplib::Headers headers = embeddingProvider.getHeaders();
    std::string path = embeddingProvider.getPath(model);

    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {

        auto text = parameters[0]->getValue<ku_string_t>(selectedPos).getAsString();


        nlohmann::json payload = embeddingProvider.getPayload(model, text);

        auto res = client.Post(path, headers, payload.dump(), "application/json");

        if (!res) {
            throw ConnectionException("Request failed: Could not connect to server\n");
        } else if (res->status != 200) {
            throw ConnectionException("Request failed with status " + std::to_string(res->status) +
                                      "\n Body: " + res->body + "\n");
        }

        auto embeddingVec = embeddingProvider.parseResponse(res);
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
    uint64_t embeddingDimensions = getInstance(StringUtils::getLower(input.arguments[1]->toString())).getEmbeddingDimension(StringUtils::getLower(input.arguments[2]->toString()));
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
