#include "providers/voyage-ai.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace llm_extension {

EmbeddingProvider& VoyageAIEmbedding::getInstance() {
    static VoyageAIEmbedding instance;
    return instance;
}

std::string VoyageAIEmbedding::getClient() const {
    return "https://api.voyageai.com";
}

std::string VoyageAIEmbedding::getPath(const std::string_view& /*model*/) const {
    return "/v1/embeddings";
}

httplib::Headers VoyageAIEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    static const std::string envVar = "VOYAGE_API_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(RuntimeException(
            "Could not get key from: " + envVar + '\n' + std::string(referenceKuzuDocs)));
    }
    return httplib::Headers{{"Content-Type", "application/json"},
        {"Authorization", "Bearer " + env_key}};
}

nlohmann::json VoyageAIEmbedding::getPayload(const std::string_view& model,
    const std::string_view& text) const {
    nlohmann::json payload = {{"model", model}, {"input", text}};
    if (dimensions.has_value()) {
        payload["output_dimension"] = dimensions.value();
    }
    return payload;
}

std::vector<float> VoyageAIEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
}

void VoyageAIEmbedding::checkModel(const std::string_view& model) const {
    static const std::unordered_set<std::string_view> validModels = {"voyage-3-large", "voyage-3.5",
        "voyage-3.5-lite", "voyage-code-3", "voyage-finance-2", "voyage-law-2", "voyage-code-2"};
    if (validModels.contains(model)) {
        return;
    }
    throw(BinderException("Invalid Model: " + std::string(model)));
}

void VoyageAIEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& region) {
    if (region.has_value()) {
        throw(BinderException("Voyage-AI does not support the region argument: " + region.value() +
                               '\n' + std::string(referenceKuzuDocs)));
    }
    this->dimensions = dimensions;
}

uint64_t VoyageAIEmbedding::getEmbeddingDimension(const std::string_view& model) const {
    static const std::unordered_map<std::string_view, uint64_t> modelDimensionMap = {
        {"voyage-3-large", 1024}, {"voyage-3.5", 1024}, {"voyage-3.5-lite", 1024},
        {"voyage-code-3", 1024}, {"voyage-finance-2", 1024}, {"voyage-law-2", 1024},
        {"voyage-code-2", 1536}};
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(BinderException("Invalid Model: " + std::string(model) + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
