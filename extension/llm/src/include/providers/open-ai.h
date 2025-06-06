#pragma once

#include <cstdint>

#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"
namespace kuzu {
namespace llm_extension {

class OpenAIEmbedding : public EmbeddingProvider {
    OpenAIEmbedding() = default;
    DELETE_COPY_AND_MOVE(OpenAIEmbedding);

public:
    ~OpenAIEmbedding() override = default;
    static EmbeddingProvider& getInstance();
    std::string getClient() const override;
    std::string getPath(const std::string&) const override;
    httplib::Headers getHeaders() const override;
    nlohmann::json getPayload(const std::string&, const std::string&) const override;
    std::vector<float> parseResponse(const httplib::Result&) const override;
    uint64_t getEmbeddingDimension(const std::string&) override;
};

} // namespace llm_extension
} // namespace kuzu
