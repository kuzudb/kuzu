#include "providers/google-gemini.h"

#include "common/exception/runtime.h"
#include "function/llm_functions.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace llm_extension {

std::shared_ptr<EmbeddingProvider> GoogleGeminiEmbedding::getInstance() {
    return std::make_shared<GoogleGeminiEmbedding>();
}

std::string GoogleGeminiEmbedding::getClient() const {
    return "https://generativelanguage.googleapis.com";
}

std::string GoogleGeminiEmbedding::getPath(const std::string& model) const {
    static const std::string envVar = "GOOGLE_GEMINI_API_KEY";
    auto env_key = main::ClientContext::getEnvVariable(envVar);
    if (env_key.empty()) {
        throw(RuntimeException("Could not read environment variable: " + envVar + "\n" +
                               std::string(referenceKuzuDocs)));
    }
    return "/v1beta/models/" + model + ":embedContent?key=" + env_key;
}

httplib::Headers GoogleGeminiEmbedding::getHeaders(const std::string& /*model*/,
    const nlohmann::json& /*payload*/) const {
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

void GoogleGeminiEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& region) {
    if (dimensions.has_value() || region.has_value()) {
        static const auto functionSignatures = CreateEmbedding::getFunctionSet();
        throw(functionSignatures[0]->signatureToString());
    }
}

} // namespace llm_extension
} // namespace kuzu
