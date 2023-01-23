#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundRenameProperty : public BoundDDL {
public:
    explicit BoundRenameProperty(
        table_id_t tableID, string tableName, property_id_t propertyID, string newName)
        : BoundDDL{StatementType::RENAME_PROPERTY, std::move(tableName)}, tableID{tableID},
          propertyID{propertyID}, newName{std::move(newName)} {}

    inline table_id_t getTableID() const { return tableID; }

    inline property_id_t getPropertyID() const { return propertyID; }

    inline string getNewName() const { return newName; }

private:
    table_id_t tableID;
    property_id_t propertyID;
    string newName;
};

} // namespace binder
} // namespace kuzu
