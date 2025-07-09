#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "function/fts_config.h"

namespace kuzu::common {
struct BufferReader;
}
namespace kuzu {
namespace fts_extension {

struct FTSIndexAuxInfo final : catalog::IndexAuxInfo {
    FTSConfig config;

    explicit FTSIndexAuxInfo(FTSConfig config) : config{std::move(config)} {}

    FTSIndexAuxInfo(const FTSIndexAuxInfo& other) : config{other.config} {}

    std::shared_ptr<common::BufferWriter> serialize() const override;
    static std::unique_ptr<FTSIndexAuxInfo> deserialize(
        std::unique_ptr<common::BufferReader> reader);

    std::unique_ptr<IndexAuxInfo> copy() override {
        return std::make_unique<FTSIndexAuxInfo>(*this);
    }

    std::string getStopWordsName(const common::FileScanInfo& exportFileInfo) const;

    std::string toCypher(const catalog::IndexCatalogEntry& indexEntry,
        const catalog::ToCypherInfo& info) const override;

    catalog::TableCatalogEntry* getTableEntryToExport(
        const main::ClientContext* context) const override;
};

struct FTSIndexCatalogEntry {
    static constexpr char TYPE_NAME[] = "FTS";
};

} // namespace fts_extension
} // namespace kuzu
