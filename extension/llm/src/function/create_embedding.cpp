#include <cstdint>
#include <cstdlib>
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
    if (provider == "open-ai")
    {
        return "https://api.openai.com";
    }

    else if (provider == "ollama")
    {
        return "http://localhost:11434";
    }
    
    throw(RuntimeException("Invalid Provider: " + provider));
    return std::string();
}


// similar to getEmbeddingDimensions, consider turning into a map
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

    else if (provider == "ollama")
    {
        return httplib::Headers{{"Content-Type", "applications/json"}};
    }
    throw(RuntimeException("Invalid Provider: " + provider));
    return httplib::Headers{};
}

// similar to getEmbeddingDimensions, consider turning into a map
static nlohmann::json getPayload(const std::string& provider, const std::string& model, const std::string& text)
{
    if (provider == "open-ai")
    {
        return nlohmann::json {{"model", model}, {"input", text}};
    }

    else if (provider == "ollama")
    {
        return nlohmann::json {{"model", model}, {"prompt", text}};
    }
    throw(RuntimeException("Invalid Provider: " + provider));
    return 0;
}

// similar to getEmbeddingDimensions, consider turning into a map
static std::string getPath(const std::string& provider)
{
    if (provider == "open-ai")
    {
        return "/v1/embeddings";
    }
    else if (provider == "ollama")
    {
        return "/api/embeddings";
    }
    throw(RuntimeException("Invalid Provider: " + provider));
    return std::string();

}

// WIP: Consider implementing as 2d map lookup
static uint64_t getEmbeddingDimensions(const std::string& provider, const std::string& model)
{
    if (provider == "open-ai")
    {
        if (model == "text-embedding-3-large")
        {
            return 3072;
        }
        else if (model == "text-embedding-3-small" || model == "text-embedding-ada-002")
        {
            return 1536;
        }
        throw(BinderException("Invalid Model: " + model));
    }

    else if (provider == "ollama")
    {
        if (model == "nomic-embed-text")
        {
            return 1536;
        }
        else if (model == "all-minilm:l6-v2")
        {
            return 384;
        }
        throw(BinderException("Invalid Model: " + model));
    }

    throw(BinderException("Invalid Provider: " + provider));
    return 0;
}


static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& /*parameterSelVectors*/,
    common::ValueVector& result, common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    
    
    auto provider = parameters[1]->getValue<ku_string_t>(0).getAsString();
    auto model = parameters[2]->getValue<ku_string_t>(0).getAsString();
    httplib::Client client(getClient(provider));
    httplib::Headers headers = getHeaders(provider);
    std::string path = getPath(provider);


    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {


        auto text = parameters[0]->getValue<ku_string_t>(selectedPos).getAsString();


        nlohmann::json payload = getPayload(provider, model, text);


        auto res = client.Post(path, headers, payload.dump(), "application/json");


        if (!res) {
            throw ConnectionException(
                "Request failed: Could not connect to server\n");
        } else if (res->status != 200) {
            throw ConnectionException("Request failed with status " + std::to_string(res->status) +
                                      "\n Body: " + res->body + "\n");
        }

        auto embeddingVec = nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
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
