#pragma once

#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundDropTable : public BoundDDL {
public:
    explicit BoundDropTable(common::table_id_t tableID, std::string tableName)
        : BoundDDL{common::StatementType::DROP_TABLE, std::move(tableName)}, tableID{tableID} {}

    inline common::table_id_t getTableID() const { return tableID; }

private:
    common::table_id_t tableID;
};

} // namespace binder
} // namespace kuzu
