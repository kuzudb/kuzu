#include "providers/ollama.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
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

std::string OllamaEmbedding::getPath(const std::string_view& /*model*/) const {
    return "/api/embeddings";
}

httplib::Headers OllamaEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    return httplib::Headers{{"Content-Type", "application/json"}};
}

nlohmann::json OllamaEmbedding::getPayload(const std::string_view& model,
    const std::string_view& text) const {
    return nlohmann::json{{"model", model}, {"prompt", text}};
}

std::vector<float> OllamaEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
}

void OllamaEmbedding::checkModel(const std::string_view& model) const {
    static const std::unordered_set<std::string_view> validModels = {"nomic-embed-text",
        "all-minilm:l6-v2"};
    if (validModels.contains(model)) {
        return;
    }
    throw(RuntimeException("Invalid Model: " + std::string(model)));
}

void OllamaEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& region) {
    if (dimensions.has_value()) {
        throw(RuntimeException("Ollama does not support the dimensions argument: " +
                               std::to_string(dimensions.value()) + '\n' +
                               std::string(referenceKuzuDocs)));
    }
    if (region.has_value()) {
        throw(RuntimeException("Ollama does not support the region argument: " + region.value() +
                               '\n' + std::string(referenceKuzuDocs)));
    }
}

uint64_t OllamaEmbedding::getEmbeddingDimension(const std::string_view& model) const {
    static const std::unordered_map<std::string_view, uint64_t> modelDimensionMap = {
        {"nomic-embed-text", 768}, {"all-minilm:l6-v2", 384}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(BinderException("Invalid Model: " + std::string(model) + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
