#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

class DropProperty : public DDL {
public:
    DropProperty(catalog::Catalog* catalog, common::table_id_t tableID,
        common::property_id_t propertyID, const DataPos& outputPos,
        storage::StorageManager& storageManager, uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::DROP_PROPERTY, catalog, outputPos, id, paramsString},
          storageManager{storageManager}, tableID{tableID}, propertyID{propertyID} {}

    void executeDDLInternal() override {
        auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
        catalog->dropProperty(tableID, propertyID);
        if (tableSchema->tableType == common::TableType::NODE) {
            auto nodesStats = storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs();
            nodesStats->removeMetadataDAHInfo(tableID, tableSchema->getColumnID(propertyID));
        }
    }

    std::string getOutputMsg() override { return {"Drop succeed."}; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropProperty>(
            catalog, tableID, propertyID, outputPos, storageManager, id, paramsString);
    }

protected:
    storage::StorageManager& storageManager;
    common::table_id_t tableID;
    common::property_id_t propertyID;
};

} // namespace processor
} // namespace kuzu
