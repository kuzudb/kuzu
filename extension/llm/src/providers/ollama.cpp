#include "providers/ollama.h"
#include "common/exception/binder.h"
#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider &OllamaEmbedding::getInstance()
{
    static OllamaEmbedding instance;
    return instance;
}
std::string OllamaEmbedding::getClient() { return "http://localhost:11434"; }
std::string OllamaEmbedding::getPath(const std::string&) { return "/api/embeddings"; }
httplib::Headers OllamaEmbedding::getHeaders()
{
    return httplib::Headers{{"Content-Type", "application/json"}};
}
nlohmann::json OllamaEmbedding::getPayload(const std::string& model, const std::string& text) 
{
    return nlohmann::json{{"model", model}, {"prompt", text}};
}
std::vector<float> OllamaEmbedding::parseResponse(const httplib::Result& res) 
{
    return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
}
uint64_t OllamaEmbedding::getEmbeddingDimension(const std::string& model) 
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
