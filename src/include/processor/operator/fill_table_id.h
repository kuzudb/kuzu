#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

// See LogicalFillTableID for its usage.
class FillTableID : public PhysicalOperator {
public:
    FillTableID(const DataPos& internalIDPos, common::table_id_t tableID,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FILL_TABLE_ID, std::move(child), id, paramsString},
          internalIDPos{internalIDPos}, tableID{tableID} {}

    inline void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* /*context*/) final {
        internalIDVector = resultSet_->getValueVector(internalIDPos).get();
    }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<FillTableID>(
            internalIDPos, tableID, children[0]->clone(), id, paramsString);
    }

private:
    DataPos internalIDPos;
    common::table_id_t tableID;
    common::ValueVector* internalIDVector;
};

} // namespace processor
} // namespace kuzu
