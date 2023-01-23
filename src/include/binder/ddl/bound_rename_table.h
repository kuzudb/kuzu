#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundRenameTable : public BoundDDL {
public:
    explicit BoundRenameTable(table_id_t tableID, string tableName, string newName)
        : BoundDDL{StatementType::RENAME_TABLE, std::move(tableName)}, tableID{tableID},
          newName{std::move(newName)} {}

    inline table_id_t getTableID() const { return tableID; }

    inline string getNewName() const { return newName; }

private:
    table_id_t tableID;
    string newName;
};

} // namespace binder
} // namespace kuzu
