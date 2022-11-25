#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class DropTable : public DDL {

public:
    DropTable(Catalog* catalog, TableSchema* tableSchema, StorageManager& storageManager,
        uint32_t id, const string& paramsString)
        : DDL{catalog, id, paramsString}, tableSchema{tableSchema}, storageManager{storageManager} {
    }

    PhysicalOperatorType getOperatorType() override { return DROP_TABLE; }

    string execute() override {
        catalog->removeTableSchema(tableSchema);
        return StringUtils::string_format("%sTable: %s has been dropped.",
            tableSchema->isNodeTable ? "Node" : "Rel", tableSchema->tableName.c_str());
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropTable>(catalog, tableSchema, storageManager, id, paramsString);
    }

protected:
    TableSchema* tableSchema;
    StorageManager& storageManager;
};

} // namespace processor
} // namespace kuzu
