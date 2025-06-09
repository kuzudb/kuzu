#pragma once

#include <string>
#include <vector>

#include "httplib.h"
#include "json.hpp"
namespace kuzu {
namespace llm_extension {

static constexpr const char* referenceKuzuDocs = "For more information, please refer to the official Kuzu documentation: https://docs.kuzudb.com/extensions/llm/\n";
class EmbeddingProvider {
public:
    virtual ~EmbeddingProvider() = default;
    virtual std::string getClient() const = 0;
    virtual std::string getPath(const std::string& model) const = 0;
    virtual httplib::Headers getHeaders(const nlohmann::json& payload) const = 0;
    virtual nlohmann::json getPayload(const std::string& model, const std::string& text) const = 0;
    virtual std::vector<float> parseResponse(const httplib::Result& res) const = 0;
    virtual uint64_t getEmbeddingDimension(const std::string& model) = 0;
};

} // namespace llm_extension
} // namespace kuzu
