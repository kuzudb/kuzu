#pragma once

#include <cstdint>
#include <unordered_map>
#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"
namespace kuzu {
namespace llm_extension {

class OpenAIEmbedding : public EmbeddingProvider {
    OpenAIEmbedding();
    DELETE_COPY_AND_MOVE(OpenAIEmbedding); 
    
    const std::unordered_map<std::string, uint64_t> modelDimensionMap = {{"text-embedding-3-large", 3072}, {"text-embedding-3-small", 1536}, {"text-embedding-ada-002", 1536}};

    public:
    ~OpenAIEmbedding() override = default;
    static EmbeddingProvider& getInstance();
    std::string getClient() override;
    std::string getPath() override;
    httplib::Headers getHeaders() override;
    nlohmann::json getPayload(const std::string&, const std::string&) override;
    std::vector<float> parseResponse(const httplib::Result&) override;
    uint64_t getEmbeddingDimension(const std::string&) override;
};

} // namespace llm_extension
} // namespace kuzu
