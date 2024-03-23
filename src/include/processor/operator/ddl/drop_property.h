#pragma once

#include "ddl.h"

namespace kuzu {
namespace processor {

class DropProperty : public DDL {
public:
    DropProperty(common::table_id_t tableID, common::property_id_t propertyID,
        const DataPos& outputPos, uint32_t id, const std::string& paramsString)
        : DDL{PhysicalOperatorType::DROP_PROPERTY, outputPos, id, paramsString}, tableID{tableID},
          propertyID{propertyID} {}

    void executeDDLInternal(ExecutionContext* context) final;

    std::string getOutputMsg() final { return {"Drop succeed."}; }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropProperty>(tableID, propertyID, outputPos, id, paramsString);
    }

protected:
    common::table_id_t tableID;
    common::property_id_t propertyID;
};

} // namespace processor
} // namespace kuzu
