#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class DropProperty : public DDL {
public:
    DropProperty(Catalog* catalog, table_id_t tableID, property_id_t propertyID,
        const DataPos& outputPos, uint32_t id, const string& paramsString)
        : DDL{PhysicalOperatorType::DROP_PROPERTY, catalog, outputPos, id, paramsString},
          tableID{tableID}, propertyID{propertyID} {}

    void executeDDLInternal() override { catalog->dropProperty(tableID, propertyID); }

    string getOutputMsg() override { return {"Drop succeed."}; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropProperty>(catalog, tableID, propertyID, outputPos, id, paramsString);
    }

protected:
    table_id_t tableID;
    property_id_t propertyID;
};

} // namespace processor
} // namespace kuzu
