#pragma once

#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"

namespace kuzu {
namespace llm_extension {

class OpenAIEmbedding final : public EmbeddingProvider {
public:
    OpenAIEmbedding() = default;
    DELETE_COPY_AND_MOVE(OpenAIEmbedding);
    ~OpenAIEmbedding() override = default;
    static std::shared_ptr<EmbeddingProvider> getInstance();
    std::string getClient() const override;
    std::string getPath(const std::string& model) const override;
    httplib::Headers getHeaders(const std::string& model,
        const nlohmann::json& payload) const override;
    nlohmann::json getPayload(const std::string& model, const std::string& text) const override;
    std::vector<float> parseResponse(const httplib::Result& res) const override;
    void configure(const std::optional<uint64_t>& dimensions,
        const std::optional<std::string>& region) override;

private:
    std::optional<uint64_t> dimensions;
};

} // namespace llm_extension
} // namespace kuzu
