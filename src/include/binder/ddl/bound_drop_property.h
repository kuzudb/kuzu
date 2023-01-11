#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundDropProperty : public BoundDDL {
public:
    explicit BoundDropProperty(table_id_t tableID, property_id_t propertyID, string tableName)
        : BoundDDL{StatementType::DROP_PROPERTY, std::move(tableName)}, tableID{tableID},
          propertyID{propertyID} {}

    inline table_id_t getTableID() const { return tableID; }

    inline property_id_t getPropertyID() const { return propertyID; }

private:
    table_id_t tableID;
    property_id_t propertyID;
};

} // namespace binder
} // namespace kuzu
