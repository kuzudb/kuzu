#pragma once

#include "httplib.h"
#include "json.hpp"

namespace kuzu {
namespace llm_extension {

class EmbeddingProvider {
public:
    // TODO(Tanvir) When docs are created validate the url in the string
    static constexpr const char* referenceKuzuDocs =
        "For more information, please refer to the official Kuzu documentation: "
        "https://docs.kuzudb.com/extensions/llm/\n";
    virtual ~EmbeddingProvider() = default;
    virtual std::string getClient() const = 0;
    virtual std::string getPath(const std::string& model) const = 0;
    virtual httplib::Headers getHeaders(const std::string& model,
        const nlohmann::json& payload) const = 0;
    virtual nlohmann::json getPayload(const std::string& model, const std::string& text) const = 0;
    virtual std::vector<float> parseResponse(const httplib::Result& res) const = 0;
    virtual void configure(const std::optional<uint64_t>& dimensions,
        const std::optional<std::string>& regionOrEndpoint) = 0;
};

} // namespace llm_extension
} // namespace kuzu
