#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "function/fts_config.h"

namespace kuzu::common {
struct BufferReader;
}
namespace kuzu {
namespace fts_extension {

struct FTSIndexAuxInfo final : catalog::IndexAuxInfo {
    common::idx_t numDocs = 0;
    double avgDocLen = 0;
    std::vector<std::string> properties;
    FTSConfig config;

    FTSIndexAuxInfo(common::idx_t numDocs, double avgDocLen, std::vector<std::string> properties,
        FTSConfig config)
        : numDocs{numDocs}, avgDocLen{avgDocLen}, properties{std::move(properties)},
          config{std::move(config)} {}

    FTSIndexAuxInfo(const FTSIndexAuxInfo& other)
        : numDocs{other.numDocs}, avgDocLen{other.avgDocLen}, properties{other.properties},
          config{other.config} {}

    std::shared_ptr<common::BufferedSerializer> serialize() const override;
    static std::unique_ptr<FTSIndexAuxInfo> deserialize(
        std::unique_ptr<common::BufferReader> reader);

    std::unique_ptr<IndexAuxInfo> copy() override {
        return std::make_unique<FTSIndexAuxInfo>(*this);
    }

    std::string toCypher(const catalog::IndexCatalogEntry& indexEntry,
        main::ClientContext* context) override;
};

struct FTSIndexCatalogEntry {
    static constexpr char TYPE_NAME[] = "FTS";
};

} // namespace fts_extension
} // namespace kuzu
