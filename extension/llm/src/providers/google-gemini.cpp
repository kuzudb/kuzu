#include "providers/google-gemini.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider &GoogleGeminiEmbedding::getInstance()
{
    static GoogleGeminiEmbedding instance;
    return instance;
}
std::string GoogleGeminiEmbedding::getClient() { return "https://generativelanguage.googleapis.com"; }
std::string GoogleGeminiEmbedding::getPath(const std::string &model)
{
    const char* envVar = "GEMINI_API_KEY";
    // NOLINTNEXTLINE thread safety warning
    auto env_key = std::getenv(envVar);
    if (env_key == nullptr) {
        throw(common::RuntimeException("Could not get key from: "+std::string(envVar)+"\n"));
    }
    return "/v1beta/models/" + model + ":embedContent?key=" + std::string(env_key);

}
httplib::Headers GoogleGeminiEmbedding::getHeaders()
{
    return httplib::Headers{{"Content-Type", "application/json"}};
}
nlohmann::json GoogleGeminiEmbedding::getPayload(const std::string &model, const std::string &text)
{
        return nlohmann::json{{"model", "models/" + model}, {"content", {{"parts", {{{"text", text}}}}}}};
}
std::vector<float> GoogleGeminiEmbedding::parseResponse(const httplib::Result &res)
{
    return nlohmann::json::parse(res->body)["embedding"]["values"].get<std::vector<float>>();
}
uint64_t GoogleGeminiEmbedding::getEmbeddingDimension(const std::string &model)
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
