#include "providers/open-ai.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider &OpenAIEmbedding::getInstance()
{
    static OpenAIEmbedding instance;
    return instance;
}
std::string OpenAIEmbedding::getClient() { return "https://api.openai.com"; }
std::string OpenAIEmbedding::getPath() { return "/v1/embeddings"; }
httplib::Headers OpenAIEmbedding::getHeaders()
{
    const char * envVar = "OPENAI_API_KEY";
    //NOLINTNEXTLINE
    auto env_key = std::getenv(envVar);
    if (env_key == nullptr)
    {
        throw(common::RuntimeException("Could not get key from: " + std::string(envVar) + '\n'));
    }
    return httplib::Headers{
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer " + std::string(env_key)}};
}
nlohmann::json OpenAIEmbedding::getPayload(const std::string& model, const std::string& text) 
{
    return nlohmann::json{{"model", model}, {"input", text}};
}
std::vector<float> OpenAIEmbedding::parseResponse(const httplib::Result& res) 
{
    return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
}
uint64_t OpenAIEmbedding::getEmbeddingDimension(const std::string& model) 
{
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end())
    {
        throw(common::BinderException("Invalid Model: " + model));
    }
    return modelDimensionMapIter->second;
        
}

} // namespace llm_extension
} // namespace kuzu
