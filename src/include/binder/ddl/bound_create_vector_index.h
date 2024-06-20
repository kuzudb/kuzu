#pragma once

#include "binder/bound_statement.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/copier_config/hnsw_reader_config.h"

namespace kuzu {
namespace binder {

class BoundCreateVectorIndex final : public BoundStatement {
public:
    explicit BoundCreateVectorIndex(catalog::TableCatalogEntry* tableEntry,
        common::property_id_t propertyID, common::HnswReaderConfig hnswReaderConfig)
        : BoundStatement{common::StatementType::CREATE_VECTOR_INDEX,
              BoundStatementResult::createSingleStringColumnResult()},
          tableEntry{tableEntry}, propertyID{propertyID}, hnswReaderConfig{hnswReaderConfig} {};

    inline catalog::TableCatalogEntry* getTableEntry() const { return tableEntry; }
    inline common::property_id_t getPropertyID() const { return propertyID; }
    inline common::HnswReaderConfig getHnswReaderConfig() const { return hnswReaderConfig; }

private:
    catalog::TableCatalogEntry* tableEntry;
    common::property_id_t propertyID;
    common::HnswReaderConfig hnswReaderConfig;
};

} // namespace binder
} // namespace kuzu
