#pragma once

#include "binder/bound_statement.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/vector_index/vector_index_config.h"

namespace kuzu {
namespace binder {

class BoundCreateVectorIndex final : public BoundStatement {
public:
    explicit BoundCreateVectorIndex(catalog::TableCatalogEntry* tableEntry,
        common::property_id_t propertyID, common::VectorIndexConfig vectorIndexConfig)
        : BoundStatement{common::StatementType::CREATE_VECTOR_INDEX,
              BoundStatementResult::createSingleStringColumnResult()},
          tableEntry{tableEntry}, propertyID{propertyID}, vectorIndexConfig{vectorIndexConfig} {};

    inline catalog::TableCatalogEntry* getTableEntry() const { return tableEntry; }
    inline common::property_id_t getPropertyID() const { return propertyID; }
    inline common::VectorIndexConfig getVectorIndexConfig() const { return vectorIndexConfig; }

private:
    catalog::TableCatalogEntry* tableEntry;
    common::property_id_t propertyID;
    common::VectorIndexConfig vectorIndexConfig;
};

} // namespace binder
} // namespace kuzu
