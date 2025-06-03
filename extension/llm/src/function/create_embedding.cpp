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
#include "json.hpp"
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace llm_extension {

// similar to getEmbeddingDimensions, consider turning into a map
static std::string getClient(const std::string& provider)
{
    static const std::unordered_map<std::string, std::string> providerClientMap = 
    {
        {"open-ai", "https://api.openai.com"},
        {"voyage-ai", "https://api.voyageai.com"},
        {"google-vertex", "https://aiplatform.googleapis.com"},
        {"google-gemini", "https://generativelanguage.googleapis.com"},
        {"ollama", "http://localhost:11434"}
    };

    auto clientIter = providerClientMap.find(provider);
    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_ASSERT(clientIter != providerClientMap.end());
    return clientIter->second;
}


static httplib::Headers getHeaders(const std::string& provider)
{
    if (provider == "open-ai")
    {
        auto env_key = std::getenv("OPENAI_API_KEY");
        if (env_key == nullptr) 
        {
            throw(RuntimeException("Could not get key from: OPENAI_API_KEY\n"));
        }
        return httplib::Headers{{"Content-Type", "application/json"}, {"Authorization", "Bearer " + std::string(env_key)}};
    }

    else if (provider == "voyage-ai")
    {
        auto env_key = std::getenv("VOYAGE_API_KEY");
        if (env_key == nullptr) 
        {
            throw(RuntimeException("Could not get key from: VOYAGE_API_KEY\n"));
        }
        return httplib::Headers{{"Content-Type", "application/json"}, {"Authorization", "Bearer " + std::string(env_key)}};
    }

    else if (provider == "google-gemini")
    {
        return httplib::Headers{{"Content-Type", "applications/json"}};
    }

    else if (provider == "google-vertex")
    {
        auto env_key = std::getenv("GOOGLE_VERTEX_ACCESS_KEY");
        if (env_key == nullptr) 
        {
            throw(RuntimeException("Could not get key from: GOOGLE_VERTEX_ACCESS_KEY\n"));
        }
        return httplib::Headers {{"Content-Type", "application/json"}, {"Authorization", "Bearer " + std::string(env_key)}};
    }

    else if (provider == "ollama")
    {
        return httplib::Headers{{"Content-Type", "applications/json"}};
    }

    // Invalid Provider Error Would Be Thrown By GetEmbeddingDimensions
    KU_UNREACHABLE;
    return httplib::Headers{};
}

static nlohmann::json getPayload(const std::string& provider, const std::string& model, const std::string& text)
{
    if (provider == "open-ai")
    {
        return nlohmann::json {{"model", model}, {"input", text}};
    }

    else if (provider == "voyage-ai")
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
    if (provider == "open-ai")
    {
        return "/v1/embeddings";
    }
    else if (provider == "voyage-ai")
    {
        return "/v1/embeddings";
    }
    else if (provider == "google-gemini")
    {
        auto env_key = std::getenv("GEMINI_API_KEY");
        if (env_key == nullptr)
        {
            throw(RuntimeException("Could not get key from: GEMINI_API_KEY\n"));
        }
        return "/v1beta/models/"+ model +":embedContent?key=" + std::string(env_key);
    }

    else if (provider == "google-vertex")
    {
        auto env_project_id = std::getenv("GOOGLE_CLOUD_PROJECT_ID");
        if (env_project_id == nullptr)
        {
            throw(RuntimeException("Could not get project id from: GOOGLE_CLOUD_PROJECT_ID\n"));
        }

        //TODO: Location is hardcoded, this should be changed
        return "/v1/projects/"+std::string(env_project_id)+"/locations/us-central1/publishers/google/models/"+model+":predict";
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
    if (provider == "open-ai")
    {
        return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
    }

    else if (provider == "voyage-ai")
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

    else if (provider == "ollama")
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
    
    
    auto provider = parameters[1]->getValue<ku_string_t>(0).getAsString();
    auto model = parameters[2]->getValue<ku_string_t>(0).getAsString();
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
#undef CPPHTTPLIB_OPENSSL_SUPPORT
