#include "providers/google-vertex.h"

#include "common/exception/binder.h"
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

std::string GoogleVertexEmbedding::getPath(const std::string_view& model) const {
    static const std::string envVar = "GOOGLE_CLOUD_PROJECT_ID";
    auto env_project_id = main::ClientContext::getEnvVariable(envVar);
    if (env_project_id.empty()) {
        throw(RuntimeException(
            "Could not get project id from: " + envVar + '\n' + std::string(referenceKuzuDocs)));
    }
    return "/v1/projects/" + env_project_id + "/locations/" + region.value_or("us-central1") +
           "/publishers/google/models/" + std::string(model) + ":predict";
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

nlohmann::json GoogleVertexEmbedding::getPayload(const std::string_view& /*model*/,
    const std::string_view& text) const {
    return nlohmann::json{{"instances", {{{"content", text}}}}};
}

std::vector<float> GoogleVertexEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["predictions"][0]["embeddings"]["values"]
        .get<std::vector<float>>();
}

void GoogleVertexEmbedding::checkModel(const std::string_view& model) const {
    static const std::unordered_set<std::string_view> validModels = {"gemini-embedding-001",
        "text-embedding-005", "text-multilingual-embedding-002"};
    if (validModels.contains(model)) {
        return;
    }
    throw(RuntimeException("Invalid Model: " + std::string(model)));
}

void GoogleVertexEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& region) {
    if (dimensions.has_value()) {
        throw(RuntimeException("Google-Vertex does not support the dimensions argument: " +
                               std::to_string(dimensions.value()) + '\n' +
                               std::string(referenceKuzuDocs)));
    }
    // Reset to default
    if (!region.has_value())
    {
        this->region = std::nullopt;
        return;
    }
    static const std::unordered_set<std::string_view> validRegions = 
    {
            "us-west1",
            "us-west2",
            "us-west3",
            "us-west4",
            "us-central1",
            "us-east1",
            "us-east4",
            "us-south1",
            "northamerica-northeast1",
            "northamerica-northeast2",
            "southamerica-east1",
            "southamerica-west1",
            "europe-west2",
            "europe-west1",
            "europe-west4",
            "europe-west6",
            "europe-west3",
            "europe-north1",
            "europe-central2",
            "europe-west8",
            "europe-west9",
            "europe-southwest1",
            "asia-south1",
            "asia-southeast1",
            "asia-southeast2",
            "asia-east2",
            "asia-east1",
            "asia-northeast1",
            "asia-northeast2",
            "australia-southeast1",
            "australia-southeast2",
            "asia-northeast3",
            "me-west1"
    };
    if (!validRegions.contains(region.value()))
    {
        throw(BinderException("Invalid Region: " + region.value() + '\n' + std::string(referenceKuzuDocs)));
    }
    this->region = region;
}

uint64_t GoogleVertexEmbedding::getEmbeddingDimension(const std::string_view& model) const {
    static const std::unordered_map<std::string_view, uint64_t> modelDimensionMap = {
        {"gemini-embedding-001", 3072}, {"text-embedding-005", 768},
        {"text-multilingual-embedding-002", 768}};

    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end()) {
        throw(BinderException("Invalid Model: " + std::string(model) + '\n' + std::string(referenceKuzuDocs)));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
