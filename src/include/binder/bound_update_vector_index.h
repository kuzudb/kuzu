#pragma once

#include "binder/bound_statement.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/vector_index/vector_index_config.h"

namespace kuzu {
namespace binder {

class BoundUpdateVectorIndex final : public BoundStatement {
public:
    explicit BoundUpdateVectorIndex(catalog::TableCatalogEntry* tableEntry,
        common::property_id_t propertyID)
        : BoundStatement{common::StatementType::UPDATE_VECTOR_INDEX,
              BoundStatementResult::createSingleStringColumnResult()},
          tableEntry{tableEntry}, propertyID{propertyID} {};

    inline catalog::TableCatalogEntry* getTableEntry() const { return tableEntry; }
    inline common::property_id_t getPropertyID() const { return propertyID; }

private:
    catalog::TableCatalogEntry* tableEntry;
    common::property_id_t propertyID;
};

} // namespace binder
} // namespace kuzu
