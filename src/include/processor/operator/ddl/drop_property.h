#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace processor {

class DropProperty : public DDL {
public:
    DropProperty(catalog::Catalog* catalog, common::table_id_t tableID,
        common::property_id_t propertyID, const DataPos& outputPos, uint32_t id,
        const std::string& paramsString)
        : DDL{PhysicalOperatorType::DROP_PROPERTY, catalog, outputPos, id, paramsString},
          tableID{tableID}, propertyID{propertyID} {}

    void executeDDLInternal() override { catalog->dropProperty(tableID, propertyID); }

    std::string getOutputMsg() override { return {"Drop succeed."}; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropProperty>(catalog, tableID, propertyID, outputPos, id, paramsString);
    }

protected:
    common::table_id_t tableID;
    common::property_id_t propertyID;
};

} // namespace processor
} // namespace kuzu
