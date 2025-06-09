#include "providers/voyage-ai.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider& VoyageAIEmbedding::getInstance() {
    static VoyageAIEmbedding instance;
    return instance;
}

std::string VoyageAIEmbedding::getClient() const {
    return "https://api.voyageai.com";
}

std::string VoyageAIEmbedding::getPath(const std::string& /*model*/) const {
    return "/v1/embeddings";
}

httplib::Headers VoyageAIEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    static const std::string envVar = "VOYAGE_API_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(common::RuntimeException("Could not get key from: " + envVar + '\n' + std::string(referenceKuzuDocs)));
    }
    return httplib::Headers{{"Content-Type", "application/json"},
        {"Authorization", "Bearer " + env_key}};
}

nlohmann::json VoyageAIEmbedding::getPayload(const std::string& model,
    const std::string& text) const {
    return nlohmann::json{{"model", model}, {"input", text}};
}

std::vector<float> VoyageAIEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
}

uint64_t VoyageAIEmbedding::getEmbeddingDimension(const std::string& model) {
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {
        {"voyage-3-large", 1024}, {"voyage-3.5", 1024}, {"voyage-3.5-lite", 1024},
        {"voyage-code-3", 1024}, {"voyage-finance-2", 1024}, {"voyage-law-2", 1024},
        {"voyage-code-2", 1536}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(common::BinderException("Invalid Model: " + model + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
