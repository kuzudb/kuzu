#pragma once

#include "catalog/catalog_entry/index_catalog_entry.h"

namespace kuzu {
namespace fts_extension {

class FTSIndexCatalogEntry final : public catalog::IndexCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    FTSIndexCatalogEntry() = default;
    FTSIndexCatalogEntry(common::table_id_t tableID, std::string indexName, common::idx_t numDocs,
        double avgDocLen, std::unordered_set<common::column_id_t> properties)
        : catalog::IndexCatalogEntry{tableID, std::move(indexName)}, numDocs{numDocs},
          avgDocLen{avgDocLen}, properties{std::move(properties)} {}

    //===--------------------------------------------------------------------===//
    // getters & setters
    //===--------------------------------------------------------------------===//
    common::idx_t getNumDocs() const { return numDocs; }
    double getAvgDocLen() const { return avgDocLen; }

    void canDropProperty() const override;

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    std::unique_ptr<catalog::IndexCatalogEntry> copy() const override;

private:
    common::idx_t numDocs = 0;
    double avgDocLen = 0;
    std::unordered_set<common::column_id_t> properties;
};

} // namespace fts_extension
} // namespace kuzu
