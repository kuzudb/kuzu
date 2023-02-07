#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class RenameTable : public DDL {
public:
    RenameTable(catalog::Catalog* catalog, common::table_id_t tableID, std::string newName,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::RENAME_TABLE, catalog, outputPos, id, paramsString},
          tableID{tableID}, newName{std::move(newName)} {}

    void executeDDLInternal() override { catalog->renameTable(tableID, newName); }

    std::string getOutputMsg() override { return "Table renamed"; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<RenameTable>(catalog, tableID, newName, outputPos, id, paramsString);
    }

protected:
    common::table_id_t tableID;
    std::string newName;
};

} // namespace processor
} // namespace kuzu
