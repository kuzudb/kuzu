#include "providers/google-vertex.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

EmbeddingProvider &GoogleVertexEmbedding::getInstance()
{
    static GoogleVertexEmbedding instance;
    return instance;
}
std::string GoogleVertexEmbedding::getClient() { return "https://aiplatform.googleapis.com"; }
std::string GoogleVertexEmbedding::getPath(const std::string &model)
{
    const char *envVar = "GOOGLE_CLOUD_PROJECT_ID";

    // NOLINTNEXTLINE thread safety warning
    auto env_project_id = std::getenv(envVar);
    if (env_project_id == nullptr)
    {
        throw(common::RuntimeException(
            "Could not get project id from: " + std::string(envVar) + "\n"));
    }

    // TODO: Location is hardcoded, this should be changed when configuration is
    // supported
    return "/v1/projects/" + std::string(env_project_id) +
           "/locations/us-central1/publishers/google/models/" + model +
           ":predict";
}
httplib::Headers GoogleVertexEmbedding::getHeaders()
{
    const char *envVar = "GOOGLE_VERTEX_ACCESS_KEY";
    // NOLINTNEXTLINE
    auto env_key = std::getenv(envVar);
    if (env_key == nullptr)
    {
        throw(common::RuntimeException(
            "Could not get key from: " + std::string(envVar) + '\n'));
    }
    return httplib::Headers{
        {"Content-Type", "application/json"},
        {"Authorization", "Bearer " + std::string(env_key)}};
}
nlohmann::json GoogleVertexEmbedding::getPayload(const std::string &, const std::string &text)
{
    return nlohmann::json{{"instances", {{{"content", text}}}}};
}
std::vector<float> GoogleVertexEmbedding::parseResponse(const httplib::Result &res)
{
    return nlohmann::json::parse(res->body)["predictions"][0]["embeddings"]["values"] .get<std::vector<float>>();
}
uint64_t GoogleVertexEmbedding::getEmbeddingDimension(const std::string &model)
{
    auto modelDimensionMapIter = modelDimensionMap.find(model);
    if (modelDimensionMapIter == modelDimensionMap.end())
    {
        throw(common::BinderException("Invalid Model: " + model));
    }
    return modelDimensionMapIter->second;
}

} // namespace llm_extension
} // namespace kuzu
