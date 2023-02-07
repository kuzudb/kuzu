#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class RenameProperty : public DDL {
public:
    RenameProperty(catalog::Catalog* catalog, common::table_id_t tableID,
        common::property_id_t propertyID, std::string newName, const DataPos& outputPos,
        uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::RENAME_PROPERTY, catalog, outputPos, id, paramsString},
          tableID{tableID}, propertyID{propertyID}, newName{std::move(newName)} {}

    void executeDDLInternal() override { catalog->renameProperty(tableID, propertyID, newName); }

    std::string getOutputMsg() override { return "Property renamed"; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<RenameProperty>(
            catalog, tableID, propertyID, newName, outputPos, id, paramsString);
    }

protected:
    common::table_id_t tableID;
    common::property_id_t propertyID;
    std::string newName;
};

} // namespace processor
} // namespace kuzu
