#include "providers/google-vertex.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"

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
        throw(common::RuntimeException(
            "Could not get project id from: " + envVar + '\n' + std::string(referenceKuzuDocs)));
    }
    // TODO(Tanvir): Location is hardcoded, this should be changed when configuration is
    // supported
    return "/v1/projects/" + env_project_id + "/locations/us-central1/publishers/google/models/" +
           model + ":predict";
}

httplib::Headers GoogleVertexEmbedding::getHeaders(const nlohmann::json& /*payload*/) const {
    static const std::string envVar = "GOOGLE_VERTEX_ACCESS_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(common::RuntimeException(
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

uint64_t GoogleVertexEmbedding::getEmbeddingDimension(const std::string& model) {
    static const std::unordered_map<std::string, uint64_t> modelDimensionMap = {
        {"gemini-embedding-001", 3072}, {"text-embedding-005", 768},
        {"text-multilingual-embedding-002", 768}};

    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(common::BinderException(
            "Invalid Model: " + model + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
