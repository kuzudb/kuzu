#pragma once

#include "src/catalog/include/catalog.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/include/storage_manager.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

class CreateNodeTable : public PhysicalOperator {

public:
    CreateNodeTable(Catalog* catalog, string labelName,
        vector<PropertyNameDataType> propertyNameDataTypes, string primaryKey,
        StorageManager* storageManager, BufferManager* bufferManager, bool inMemoryMode,
        string databasePath, uint32_t id, const string& paramsString)
        : PhysicalOperator{nullptr, id, paramsString}, catalog{catalog}, labelName{move(labelName)},
          propertyNameDataTypes{move(propertyNameDataTypes)}, primaryKey{move(primaryKey)},
          storageManager{storageManager}, bufferManager{bufferManager}, inMemoryMode{inMemoryMode},
          databasePath{move(databasePath)} {}

    void execute();

    PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::CREATE_NODE_TABLE;
    }

    bool getNextTuples() override { return true; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<CreateNodeTable>(catalog, labelName, propertyNameDataTypes, primaryKey,
            storageManager, bufferManager, inMemoryMode, databasePath, id, paramsString);
    }

private:
    Catalog* catalog;
    string labelName;
    vector<PropertyNameDataType> propertyNameDataTypes;
    string primaryKey;
    StorageManager* storageManager;
    BufferManager* bufferManager;
    bool inMemoryMode;
    string databasePath;
};

} // namespace processor
} // namespace graphflow
