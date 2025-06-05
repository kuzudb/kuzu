#pragma once

#include <cstdint>
#include <unordered_map>
#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"
namespace kuzu {
namespace llm_extension {

class GoogleGeminiEmbedding : public EmbeddingProvider {
    GoogleGeminiEmbedding() = default;
    DELETE_COPY_AND_MOVE(GoogleGeminiEmbedding); 
    
    const std::unordered_map<std::string, uint64_t> modelDimensionMap = {{"gemini-embedding-exp-03-07", 3072}, {"text-embedding-004", 768}, {"embedding-001", 768}};

    public:
    ~GoogleGeminiEmbedding() override = default;
    static EmbeddingProvider& getInstance();
    std::string getClient() override;
    std::string getPath(const std::string&) override;
    httplib::Headers getHeaders() override;
    nlohmann::json getPayload(const std::string&, const std::string&) override;
    std::vector<float> parseResponse(const httplib::Result&) override;
    uint64_t getEmbeddingDimension(const std::string&) override;
};

} // namespace llm_extension
} // namespace kuzu
