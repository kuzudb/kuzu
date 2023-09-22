#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

// LogicalFillTableID is a temporary solution for reading predicate IRI in RDF.
// Predicate internal ID is stored in both RDF rel (triple) table and RDF node (resource) table.
// Their offset can match with each other, but since we don't materialize table ID, the two columns'
// table ID is determined by rel and node table respectively.
// This operator allows overwriting table ID for a given internal ID vector.
// DO NOT USE IN NON-RDF PROCESSING.
class LogicalFillTableID : public LogicalOperator {
public:
    LogicalFillTableID(std::shared_ptr<binder::Expression> internalID, common::table_id_t tableID,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FILL_TABLE_ID, std::move(child)},
          internalID{std::move(internalID)}, tableID{tableID} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const final { return internalID->toString(); }

    inline std::shared_ptr<binder::Expression> getInternalID() const { return internalID; }
    inline common::table_id_t getTableID() const { return tableID; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalFillTableID>(internalID, tableID, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> internalID;
    common::table_id_t tableID;
};

} // namespace planner
} // namespace kuzu
