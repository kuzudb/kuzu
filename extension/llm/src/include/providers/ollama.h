#pragma once

#include <cstdint>

#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"
namespace kuzu {
namespace llm_extension {

class OllamaEmbedding : public EmbeddingProvider {
    OllamaEmbedding() = default;
    DELETE_COPY_AND_MOVE(OllamaEmbedding);

public:
    ~OllamaEmbedding() override = default;
    static EmbeddingProvider& getInstance();
    std::string getClient() const override;
    std::string getPath(const std::string& model) const override;
    httplib::Headers getHeaders() const override;
    nlohmann::json getPayload(const std::string& model, const std::string& text) const override;
    std::vector<float> parseResponse(const httplib::Result& res) const override;
    uint64_t getEmbeddingDimension(const std::string& model) override;
};

} // namespace llm_extension
} // namespace kuzu
