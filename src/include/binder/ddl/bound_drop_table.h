#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundDropTable : public BoundDDL {
public:
    explicit BoundDropTable(table_id_t tableID, string tableName)
        : BoundDDL{StatementType::DROP_TABLE, std::move(tableName)}, tableID{tableID} {}

    inline table_id_t getTableID() const { return tableID; }

private:
    table_id_t tableID;
};

} // namespace binder
} // namespace kuzu
