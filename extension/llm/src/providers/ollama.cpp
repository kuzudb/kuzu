#include "providers/ollama.h"

#include "function/llm_functions.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace llm_extension {

std::shared_ptr<EmbeddingProvider> OllamaEmbedding::getInstance() {
    return std::make_shared<OllamaEmbedding>();
}

std::string OllamaEmbedding::getClient() const {
    return endpoint.value_or("http://localhost:11434");
}

std::string OllamaEmbedding::getPath(const std::string& /*model*/) const {
    return "/api/embeddings";
}

httplib::Headers OllamaEmbedding::getHeaders(const std::string& /*model*/,
    const nlohmann::json& /*payload*/) const {
    return httplib::Headers{{"Content-Type", "application/json"}};
}

nlohmann::json OllamaEmbedding::getPayload(const std::string& model,
    const std::string& text) const {
    return nlohmann::json{{"model", model}, {"prompt", text}};
}

std::vector<float> OllamaEmbedding::parseResponse(const httplib::Result& res) const {
    return nlohmann::json::parse(res->body)["embedding"].get<std::vector<float>>();
}

void OllamaEmbedding::configure(const std::optional<uint64_t>& dimensions,
    const std::optional<std::string>& endpoint) {
    static const std::string envVarOllamaUrl = "OLLAMA_URL";
    if (dimensions.has_value()) {
        static const auto functionSignatures = CreateEmbedding::getFunctionSet();
        throw(functionSignatures[0]->signatureToString() + '\n' +
              functionSignatures[1]->signatureToString());
    }
    this->endpoint = endpoint;
    if (endpoint.has_value()) {
        return;
    }
    auto envOllamaUrl = main::ClientContext::getEnvVariable(envVarOllamaUrl);
    if (!envOllamaUrl.empty()) {
        this->endpoint = envOllamaUrl;
    }
}

} // namespace llm_extension
} // namespace kuzu
