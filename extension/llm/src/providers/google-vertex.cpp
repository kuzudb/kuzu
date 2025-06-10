#include "providers/google-vertex.h"

#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace llm_extension {

EmbeddingProvider& GoogleVertexEmbedding::getInstance() {
    static GoogleVertexEmbedding instance;
    return instance;
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
    return "/v1/projects/" + env_project_id + "/locations/"+region.value_or("us-central1")+"/publishers/google/models/" +
           model + ":predict";
}

httplib::Headers GoogleVertexEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    static const std::string envVar = "GOOGLE_VERTEX_ACCESS_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(RuntimeException(
            "Could not get key from: " + envVar + '\n' + std::string(referenceKuzuDocs)));
    }
    return httplib::Headers{{"Content-Type", "application/json"},
        {"Authorization", "Bearer " + env_key}};
}

nlohmann::json GoogleVertexEmbedding::getPayload(const std::string& /*model*/,
    const std::string& text) const {
    return nlohmann::json{{"instances", {{{"content", text}}}}};
}

std::vector<float> GoogleVertexEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["predictions"][0]["embeddings"]["values"]
        .get<std::vector<float>>();
}

void GoogleVertexEmbedding::configure(const std::optional<uint64_t>& dimensions, const std::optional<std::string>& region) 
{
    if (dimensions.has_value())
    {
        throw(RuntimeException("Google-Vertex does not support the dimensions argument: " + std::to_string(dimensions.value()) + '\n' + std::string(referenceKuzuDocs)));
    }
    this->region = region;
}

} // namespace llm_extension
} // namespace kuzu
