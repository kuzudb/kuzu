#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "storage/index/hnsw_config.h"

namespace kuzu::common {
struct BufferReader;
}
namespace kuzu {
namespace catalog {

struct HNSWIndexAuxInfo final : IndexAuxInfo {

    common::table_id_t upperRelTableID = common::INVALID_TABLE_ID;
    common::table_id_t lowerRelTableID = common::INVALID_TABLE_ID;
    common::offset_t upperEntryPoint = common::INVALID_OFFSET;
    common::offset_t lowerEntryPoint = common::INVALID_OFFSET;
    storage::HNSWIndexConfig config;

    HNSWIndexAuxInfo(common::table_id_t upperRelTableID, common::table_id_t lowerRelTableID,
        common::offset_t upperEntryPoint, common::offset_t lowerEntryPoint,
        storage::HNSWIndexConfig config)
        : upperRelTableID{upperRelTableID}, lowerRelTableID{lowerRelTableID},
          upperEntryPoint{upperEntryPoint}, lowerEntryPoint{lowerEntryPoint},
          config{std::move(config)} {}

    HNSWIndexAuxInfo(const HNSWIndexAuxInfo& other)
        : upperRelTableID{other.upperRelTableID}, lowerRelTableID{other.lowerRelTableID},
          upperEntryPoint{other.upperEntryPoint}, lowerEntryPoint{other.lowerEntryPoint},
          config{other.config.copy()} {}

    std::shared_ptr<common::BufferedSerializer> serialize() const override;
    static std::unique_ptr<HNSWIndexAuxInfo> deserialize(
        std::unique_ptr<common::BufferReader> reader);

    std::unique_ptr<IndexAuxInfo> copy() override {
        return std::make_unique<HNSWIndexAuxInfo>(*this);
    }

    std::string toCypher(const IndexCatalogEntry& indexEntry,
        const ToCypherInfo& info) const override;
};

struct HNSWIndexCatalogEntry {
    static constexpr char TYPE_NAME[] = "HNSW";

    static std::string toCypher(const IndexCatalogEntry& indexEntry, main::ClientContext* context,
        const common::FileScanInfo& exportFileInfo);
};

} // namespace catalog
} // namespace kuzu
