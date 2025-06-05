#pragma once

#include <cstdint>
#include <unordered_map>
#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"
namespace kuzu {
namespace llm_extension {

class BedrockEmbedding : public EmbeddingProvider {
    BedrockEmbedding();
    DELETE_COPY_AND_MOVE(BedrockEmbedding); 
    
    const std::unordered_map<std::string, uint64_t> modelDimensionMap = {{"amazon.titan-embed-text-v1", 1024}};

    public:
    ~BedrockEmbedding() override = default;
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
