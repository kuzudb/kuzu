#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundRenameTable : public BoundDDL {
public:
    explicit BoundRenameTable(
        common::table_id_t tableID, std::string tableName, std::string newName)
        : BoundDDL{common::StatementType::RENAME_TABLE, std::move(tableName)}, tableID{tableID},
          newName{std::move(newName)} {}

    inline common::table_id_t getTableID() const { return tableID; }

    inline std::string getNewName() const { return newName; }

private:
    common::table_id_t tableID;
    std::string newName;
};

} // namespace binder
} // namespace kuzu
