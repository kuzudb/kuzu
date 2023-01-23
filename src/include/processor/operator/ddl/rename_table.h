#pragma once

#include "ddl.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class RenameTable : public DDL {
public:
    RenameTable(Catalog* catalog, table_id_t tableID, string newName, const DataPos& outputPos,
        uint32_t id, const string& paramsString)
        : DDL{PhysicalOperatorType::RENAME_TABLE, catalog, outputPos, id, paramsString},
          tableID{tableID}, newName{std::move(newName)} {}

    void executeDDLInternal() override { catalog->renameTable(tableID, newName); }

    std::string getOutputMsg() override { return "Table renamed"; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<RenameTable>(catalog, tableID, newName, outputPos, id, paramsString);
    }

protected:
    table_id_t tableID;
    string newName;
};

} // namespace processor
} // namespace kuzu
