#include "providers/amazon-bedrock.h"
#include "common/exception/binder.h"
#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider &BedrockEmbedding::getInstance()
{
    static BedrockEmbedding instance;
    return instance;
}
std::string BedrockEmbedding::getClient() const { return "https://bedrock-runtime.us-east-1.amazonaws.com"; }
std::string BedrockEmbedding::getPath(const std::string&) const { return "/model/amazon.titan-embed-text-v1/invoke"; }
httplib::Headers BedrockEmbedding::getHeaders() const
{
    //WIP
    return {};
}
nlohmann::json BedrockEmbedding::getPayload(const std::string&, const std::string& text) const
{
    return nlohmann::json{{"inputText", text}};
}
std::vector<float> BedrockEmbedding::parseResponse(const httplib::Result& res) const
{
    return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
}
uint64_t BedrockEmbedding::getEmbeddingDimension(const std::string& model) 
{
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {{"amazon.titan-embed-text-v1", 1024}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end())
    {
        throw(common::BinderException("Invalid Model: " + model));
    }
    return modelDimensionMapIter->second;
        
}

} // namespace llm_extension
} // namespace kuzu
