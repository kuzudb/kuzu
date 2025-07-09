#include "providers/google-vertex.h"

#include "common/exception/runtime.h"
#include "function/llm_functions.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace llm_extension {

std::shared_ptr<EmbeddingProvider> GoogleVertexEmbedding::getInstance() {
    return std::make_shared<GoogleVertexEmbedding>();
}

std::string GoogleVertexEmbedding::getClient() const {
    return "https://aiplatform.googleapis.com";
}

std::string GoogleVertexEmbedding::getPath(const std::string& model) const {
    static const std::string envVar = "GOOGLE_CLOUD_PROJECT_ID";
    auto env_project_id = main::ClientContext::getEnvVariable(envVar);
    if (env_project_id.empty()) {
        throw(RuntimeException(
            "Could not get project id from: " + envVar + '\n' + std::string(referenceKuzuDocs)));
    }
    return "/v1/projects/" + env_project_id + "/locations/" + region.value_or("") +
           "/publishers/google/models/" + model + ":predict";
}

httplib::Headers GoogleVertexEmbedding::getHeaders(const std::string& /*model*/,
    const nlohmann::json& /*payload*/) const {
    static const std::string envVar = "GOOGLE_VERTEX_ACCESS_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(RuntimeException("Could not read environmental variable: " + envVar + '\n' +
                               std::string(referenceKuzuDocs)));
    }
    return httplib::Headers{{"Content-Type", "application/json"},
        {"Authorization", "Bearer " + env_key}};
}

nlohmann::json GoogleVertexEmbedding::getPayload(const std::string& /*model*/,
    const std::string& text) const {
    nlohmann::json payload{
        {"instances", {{{"content", text}, {"task_type", "RETRIEVAL_DOCUMENT"}}}}};
    if (dimensions.has_value()) {
        payload["parameters"] = {{"outputDimensionality", dimensions.value()}};
    }
    return payload;
}

std::vector<float> GoogleVertexEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["predictions"][0]["embeddings"]["values"]
        .get<std::vector<float>>();
}

void GoogleVertexEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& region) {
    if (!region.has_value()) {
        static const auto functionSignatures = CreateEmbedding::getFunctionSet();
        throw(functionSignatures[1]->signatureToString() + '\n' +
              functionSignatures[3]->signatureToString());
    }
    this->dimensions = dimensions;
    this->region = region;
}

} // namespace llm_extension
} // namespace kuzu
