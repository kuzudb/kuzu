#pragma once

#include "bound_create_table_info.h"
#include "bound_ddl.h"

namespace kuzu {
namespace binder {

class BoundCreateTable : public BoundDDL {
public:
    explicit BoundCreateTable(std::string tableName, std::unique_ptr<BoundCreateTableInfo> info)
        : BoundDDL{common::StatementType::CREATE_TABLE, std::move(tableName)}, info{std::move(
                                                                                   info)} {}

    inline BoundCreateTableInfo* getBoundCreateTableInfo() const { return info.get(); }

private:
    std::unique_ptr<BoundCreateTableInfo> info;
};

} // namespace binder
} // namespace kuzu
