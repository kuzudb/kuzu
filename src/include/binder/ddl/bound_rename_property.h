#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundRenameProperty : public BoundDDL {
public:
    explicit BoundRenameProperty(common::table_id_t tableID, std::string tableName,
        common::property_id_t propertyID, std::string newName)
        : BoundDDL{common::StatementType::RENAME_PROPERTY, std::move(tableName)}, tableID{tableID},
          propertyID{propertyID}, newName{std::move(newName)} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline common::property_id_t getPropertyID() const { return propertyID; }

    inline std::string getNewName() const { return newName; }

private:
    common::table_id_t tableID;
    common::property_id_t propertyID;
    std::string newName;
};

} // namespace binder
} // namespace kuzu
