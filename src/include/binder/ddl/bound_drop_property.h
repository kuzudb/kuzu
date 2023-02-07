#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundDropProperty : public BoundDDL {
public:
    explicit BoundDropProperty(
        common::table_id_t tableID, common::property_id_t propertyID, std::string tableName)
        : BoundDDL{common::StatementType::DROP_PROPERTY, std::move(tableName)}, tableID{tableID},
          propertyID{propertyID} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline common::property_id_t getPropertyID() const { return propertyID; }

private:
    common::table_id_t tableID;
    common::property_id_t propertyID;
};

} // namespace binder
} // namespace kuzu
