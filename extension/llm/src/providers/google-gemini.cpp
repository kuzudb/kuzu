#include "providers/google-gemini.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"

namespace kuzu {
namespace llm_extension {

EmbeddingProvider& GoogleGeminiEmbedding::getInstance() {
    static GoogleGeminiEmbedding instance;
    return instance;
}

std::string GoogleGeminiEmbedding::getClient() const {
    return "https://generativelanguage.googleapis.com";
}

std::string GoogleGeminiEmbedding::getPath(const std::string& model) const {
    static const std::string envVar = "GEMINI_API_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(common::RuntimeException(
            "Could not get key from: " + envVar + "\n" + std::string(referenceKuzuDocs)));
    }
    return "/v1beta/models/" + model + ":embedContent?key=" + env_key;
}

httplib::Headers GoogleGeminiEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    return httplib::Headers{{"Content-Type", "application/json"}};
}

nlohmann::json GoogleGeminiEmbedding::getPayload(const std::string& model,
    const std::string& text) const {
    return nlohmann::json{{"model", "models/" + model},
        {"content", {{"parts", {{{"text", text}}}}}}};
}

std::vector<float> GoogleGeminiEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["embedding"]["values"].get<std::vector<float>>();
}

uint64_t GoogleGeminiEmbedding::getEmbeddingDimension(const std::string& model) {
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {
        {"gemini-embedding-exp-03-07", 3072}, {"text-embedding-004", 768}, {"embedding-001", 768}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(common::BinderException(
            "Invalid Model: " + model + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
