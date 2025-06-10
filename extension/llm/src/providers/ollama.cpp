#include "providers/ollama.h"

#include "common/exception/binder.h"
#include "httplib.h"
#include "json.hpp"

using namespace kuzu::common;

namespace kuzu {
namespace llm_extension {

EmbeddingProvider& OllamaEmbedding::getInstance() {
    static OllamaEmbedding instance;
    return instance;
}

std::string OllamaEmbedding::getClient() const {
    return "http://localhost:11434";
}

std::string OllamaEmbedding::getPath(const std::string& /*model*/) const {
    return "/api/embeddings";
}

httplib::Headers OllamaEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    return httplib::Headers{{"Content-Type", "application/json"}};
}

nlohmann::json OllamaEmbedding::getPayload(const std::string& model,
    const std::string& text) const {
    return nlohmann::json{{"model", model}, {"prompt", text}};
}

std::vector<float> OllamaEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
}

uint64_t OllamaEmbedding::getEmbeddingDimension(const std::string& model) {
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {
        {"nomic-embed-text", 768}, {"all-minilm:l6-v2", 384}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(BinderException(
            "Invalid Model: " + model + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

void OllamaEmbedding::configure(const std::optional<uint64_t>& dimensions, const std::optional<std::string>& region) 
{
    if (dimensions.has_value())
    {
        throw(BinderException("Google-Gemini does not support the dimensions argument: " + std::to_string(dimensions.value()) + '\n' + std::string(referenceKuzuDocs)));
    }
    if (region.has_value())
    {
        throw(BinderException("Google-Gemini does not support the region argument: " + region.value() + '\n' + std::string(referenceKuzuDocs)));
    }
}

} // namespace llm_extension
} // namespace kuzu
