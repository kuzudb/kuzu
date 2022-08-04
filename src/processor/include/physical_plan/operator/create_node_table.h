#pragma once

#include "ddl.h"

#include "src/storage/store/include/nodes_metadata.h"

namespace graphflow {
namespace processor {

class CreateNodeTable : public DDL {

public:
    CreateNodeTable(Catalog* catalog, string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes, uint32_t primaryKeyIdx, uint32_t id,
        const string& paramsString, NodesMetadata* nodesMetadata)
        : DDL{catalog, move(labelName), move(propertyNameDataTypes), id, paramsString},
          primaryKeyIdx{primaryKeyIdx}, nodesMetadata{nodesMetadata} {}

    PhysicalOperatorType getOperatorType() override { return CREATE_NODE_TABLE; }

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNodeTable>(catalog, labelName, propertyNameDataTypes,
            primaryKeyIdx, id, paramsString, nodesMetadata);
    }

private:
    uint32_t primaryKeyIdx;
    NodesMetadata* nodesMetadata;
};

} // namespace processor
} // namespace graphflow
