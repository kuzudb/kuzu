#include "providers/open-ai.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider& OpenAIEmbedding::getInstance() {
    static OpenAIEmbedding instance;
    return instance;
}

std::string OpenAIEmbedding::getClient() const {
    return "https://api.openai.com";
}

std::string OpenAIEmbedding::getPath(const std::string& /*model*/) const {
    return "/v1/embeddings";
}

httplib::Headers OpenAIEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    static const std::string envVar = "OPENAI_API_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(common::RuntimeException("Could not get key from: " + envVar + '\n'));
    }
    return httplib::Headers{{"Content-Type", "application/json"},
        {"Authorization", "Bearer " + env_key}};
}

nlohmann::json OpenAIEmbedding::getPayload(const std::string& model,
    const std::string& text) const {
    return nlohmann::json{{"model", model}, {"input", text}};
}

std::vector<float> OpenAIEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
}

uint64_t OpenAIEmbedding::getEmbeddingDimension(const std::string& model) {
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {
        {"text-embedding-3-large", 3072}, {"text-embedding-3-small", 1536},
        {"text-embedding-ada-002", 1536}};

    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(common::BinderException("Invalid Model: " + model));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
