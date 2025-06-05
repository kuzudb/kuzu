#pragma once

#include <string>
#include <vector>
#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

class EmbeddingProvider {
    public:
    virtual ~EmbeddingProvider() = default;
    virtual std::string getClient() = 0;
    virtual std::string getPath(const std::string&) = 0;
    virtual httplib::Headers getHeaders() = 0;
    virtual nlohmann::json getPayload(const std::string&, const std::string&) = 0;
    virtual std::vector<float> parseResponse(const httplib::Result&) = 0;
    virtual uint64_t getEmbeddingDimension(const std::string& model) = 0;
};

} // namespace llm_extension
} // namespace kuzu
