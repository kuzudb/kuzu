#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class DropTable : public DDL {
public:
    DropTable(Catalog* catalog, table_id_t tableID, StorageManager& storageManager, uint32_t id,
        const string& paramsString)
        : DDL{PhysicalOperatorType::DROP_TABLE, catalog, id, paramsString}, tableID{tableID},
          storageManager{storageManager} {}

    string execute() override {
        auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
        catalog->removeTableSchema(tableSchema);
        return StringUtils::string_format("%sTable: %s has been dropped.",
            tableSchema->isNodeTable ? "Node" : "Rel", tableSchema->tableName.c_str());
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(catalog, tableID, storageManager, id, paramsString);
    }

protected:
    table_id_t tableID;
    StorageManager& storageManager;
};

} // namespace processor
} // namespace kuzu
