#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"
#include "function/fts_config.h"

namespace kuzu {
namespace fts_extension {

class FTSIndexCatalogEntry final : public catalog::IndexCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    FTSIndexCatalogEntry() = default;
    FTSIndexCatalogEntry(common::table_id_t tableID, std::string indexName, common::idx_t numDocs,
        double avgDocLen, FTSConfig config)
        : catalog::IndexCatalogEntry{TYPE_NAME, tableID, std::move(indexName)}, numDocs{numDocs},
          avgDocLen{avgDocLen}, config{std::move(config)} {}

    //===--------------------------------------------------------------------===//
    // getters & setters
    //===--------------------------------------------------------------------===//
    common::idx_t getNumDocs() const { return numDocs; }
    double getAvgDocLen() const { return avgDocLen; }
    const FTSConfig& getFTSConfig() const { return config; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    std::unique_ptr<catalog::IndexCatalogEntry> copy() const override;

    void serializeAuxInfo(common::Serializer& serializer) const override;

    static std::unique_ptr<FTSIndexCatalogEntry> deserializeAuxInfo(
        catalog::IndexCatalogEntry* indexCatalogEntry);

public:
    static constexpr char TYPE_NAME[] = "FTS";

private:
    common::idx_t numDocs = 0;
    double avgDocLen = 0;
    FTSConfig config;
};

} // namespace fts_extension
} // namespace kuzu
