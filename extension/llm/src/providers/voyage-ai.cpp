#include "providers/voyage-ai.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
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

std::string VoyageAIEmbedding::getPath(const std::string& /*model*/) const {
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

nlohmann::json VoyageAIEmbedding::getPayload(const std::string& model,
    const std::string& text) const {
    nlohmann::json payload = {{"model", model}, {"input", text}};
    if (dimensions.has_value()) {
        payload["output_dimension"] = dimensions.value();
    }
    return payload;
}

std::vector<float> VoyageAIEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["data"][0]["embedding"].get<std::vector<float>>();
}

void VoyageAIEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& region) {
    if (region.has_value()) {
        throw(BinderException("Voyage-AI does not support the region argument: " + region.value() +
                              '\n' + std::string(referenceKuzuDocs)));
    }
    this->dimensions = dimensions;
}

} // namespace llm_extension
} // namespace kuzu
