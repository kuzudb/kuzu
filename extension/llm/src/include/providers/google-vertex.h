#pragma once

#include <cstdint>

#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"

namespace kuzu {
namespace llm_extension {

class GoogleVertexEmbedding final : public EmbeddingProvider {
    GoogleVertexEmbedding() = default;
    DELETE_COPY_AND_MOVE(GoogleVertexEmbedding);
    std::optional<std::string> region;

public:
    ~GoogleVertexEmbedding() override = default;
    static EmbeddingProvider& getInstance();
    std::string getClient() const override;
    std::string getPath(const std::string& model) const override;
    httplib::Headers getHeaders(const nlohmann::json& payload) const override;
    nlohmann::json getPayload(const std::string& model, const std::string& text) const override;
    std::vector<float> parseResponse(const httplib::Result& res) const override;
    void configure(const std::optional<uint64_t>& dimensions, const std::optional<std::string>& region) override;
};

} // namespace llm_extension
} // namespace kuzu
