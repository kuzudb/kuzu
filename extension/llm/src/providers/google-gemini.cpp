#include "providers/google-gemini.h"

#include "common/exception/runtime.h"
#include "httplib.h"
#include "json.hpp"
#include "main/client_context.h"

using namespace kuzu::common;

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
        throw(RuntimeException(
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

void GoogleGeminiEmbedding::configure(const std::optional<uint64_t>& dimensions, const std::optional<std::string>& region) 
{
    if (dimensions.has_value())
    {
        throw(RuntimeException("Google-Gemini does not support the dimensions argument: " + std::to_string(dimensions.value()) + '\n' + std::string(referenceKuzuDocs)));
    }
    if (region.has_value())
    {
        throw(RuntimeException("Google-Gemini does not support the region argument: " + region.value() + '\n' + std::string(referenceKuzuDocs)));
    }
}

} // namespace llm_extension
} // namespace kuzu
