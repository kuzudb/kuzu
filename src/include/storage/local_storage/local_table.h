#pragma once

#include <map>

#include "common/enums/table_type.h"
#include "common/types/internal_id_t.h"
#include "storage/store/node_group_collection.h"

namespace kuzu {
namespace storage {

using offset_to_row_idx_t = std::map<common::offset_t, common::row_idx_t>;
using offset_to_row_idx_vec_t = std::map<common::offset_t, std::vector<common::row_idx_t>>;
using offset_set_t = std::unordered_set<common::offset_t>;

struct TableInsertState;
struct TableUpdateState;
struct TableDeleteState;
class Table;
class LocalTable {
public:
    virtual ~LocalTable() = default;

    virtual bool insert(transaction::Transaction* transaction, TableInsertState& insertState) = 0;
    virtual bool update(TableUpdateState& updateState) = 0;
    virtual bool delete_(transaction::Transaction* transaction, TableDeleteState& deleteState) = 0;
    virtual bool addColumn(transaction::Transaction* transaction,
        TableAddColumnState& addColumnState) = 0;
    virtual void clear() = 0;
    virtual common::TableType getTableType() const = 0;

    template<class TARGET>
    const TARGET& constCast() {
        return common::ku_dynamic_cast<const LocalTable&, const TARGET&>(*this);
    }
    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<LocalTable&, TARGET&>(*this);
    }
    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<LocalTable*, TARGET*>(this);
    }
    template<class TARGET>
    const TARGET* ptrCast() const {
        return common::ku_dynamic_cast<LocalTable*, TARGET*>(this);
    }

protected:
    explicit LocalTable(Table& table) : table{table} {}

protected:
    Table& table;
};

} // namespace storage
} // namespace kuzu
