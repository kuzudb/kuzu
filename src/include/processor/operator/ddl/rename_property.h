#pragma once

#include "ddl.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class RenameProperty : public DDL {
public:
    RenameProperty(Catalog* catalog, table_id_t tableID, property_id_t propertyID, string newName,
        const DataPos& outputPos, uint32_t id, const string& paramsString)
        : DDL{PhysicalOperatorType::RENAME_PROPERTY, catalog, outputPos, id, paramsString},
          tableID{tableID}, propertyID{propertyID}, newName{std::move(newName)} {}

    void executeDDLInternal() override { catalog->renameProperty(tableID, propertyID, newName); }

    std::string getOutputMsg() override { return "Property renamed"; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<RenameProperty>(
            catalog, tableID, propertyID, newName, outputPos, id, paramsString);
    }

protected:
    table_id_t tableID;
    property_id_t propertyID;
    string newName;
};

} // namespace processor
} // namespace kuzu
