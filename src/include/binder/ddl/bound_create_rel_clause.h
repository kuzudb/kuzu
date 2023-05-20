#pragma once

#include "bound_create_table.h"

namespace kuzu {
namespace binder {

class BoundCreateRelClause : public BoundCreateTable {
public:
    BoundCreateRelClause(std::string tableName, std::vector<catalog::Property> properties,
        catalog::RelMultiplicity relMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID)
        : BoundCreateTable{common::StatementType::CREATE_REL_TABLE, std::move(tableName),
              std::move(properties)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {}

    inline catalog::RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline common::table_id_t getSrcTableID() const { return srcTableID; }

    inline common::table_id_t getDstTableID() const { return dstTableID; }

private:
    catalog::RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};

} // namespace binder
} // namespace kuzu
