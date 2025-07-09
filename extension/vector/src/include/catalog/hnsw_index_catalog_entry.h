#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "index/hnsw_config.h"

namespace kuzu::common {
struct BufferReader;
} // namespace kuzu::common

namespace kuzu {
namespace vector_extension {

struct HNSWIndexAuxInfo final : catalog::IndexAuxInfo {

    HNSWIndexConfig config;

    explicit HNSWIndexAuxInfo(HNSWIndexConfig config) : config{std::move(config)} {}

    HNSWIndexAuxInfo(const HNSWIndexAuxInfo& other) : config{other.config.copy()} {}

    std::shared_ptr<common::BufferWriter> serialize() const override;
    static std::unique_ptr<HNSWIndexAuxInfo> deserialize(
        std::unique_ptr<common::BufferReader> reader);

    std::unique_ptr<IndexAuxInfo> copy() override {
        return std::make_unique<HNSWIndexAuxInfo>(*this);
    }

    std::string toCypher(const catalog::IndexCatalogEntry& indexEntry,
        const catalog::ToCypherInfo& info) const override;
};

struct HNSWIndexCatalogEntry {
    static constexpr char TYPE_NAME[] = "HNSW";

    static std::string toCypher(const catalog::IndexCatalogEntry& indexEntry,
        main::ClientContext* context, const common::FileScanInfo& exportFileInfo);
};

} // namespace vector_extension
} // namespace kuzu
