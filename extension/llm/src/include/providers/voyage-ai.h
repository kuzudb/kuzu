#pragma once

#include <cstdint>
#include <unordered_map>
#include "common/copy_constructors.h"
#include "httplib.h"
#include "json.hpp"
#include "provider.h"
namespace kuzu {
namespace llm_extension {

class VoyageAIEmbedding : public EmbeddingProvider {
    VoyageAIEmbedding();
    DELETE_COPY_AND_MOVE(VoyageAIEmbedding); 
    
    const std::unordered_map<std::string, uint64_t> modelDimensionMap = {{"voyage-3-large", 1024}, {"voyage-3.5", 1024}, {"voyage-3.5-lite", 1024}, {"voyage-code-3", 1024}, {"voyage-finance-2", 1024}, {"voyage-law-2", 1024}, {"voyage-code-2", 1536}};

    public:
    ~VoyageAIEmbedding() override = default;
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
