#pragma once

#include "processor/operator/scan/scan_rel_table.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

class RelTableCollection {
public:
    RelTableCollection(
        vector<DirectedRelTableData*> tables, vector<unique_ptr<RelTableScanState>> tableScanStates)
        : tables{std::move(tables)}, tableScanStates{std::move(tableScanStates)} {}

    void resetState();
    inline uint32_t getNumTablesInCollection() { return tables.size(); }

    bool scan(const shared_ptr<ValueVector>& inVector,
        vector<shared_ptr<ValueVector>>& outputVectors, Transaction* transaction);

    unique_ptr<RelTableCollection> clone() const;

private:
    vector<DirectedRelTableData*> tables;
    vector<unique_ptr<RelTableScanState>> tableScanStates;

    uint32_t currentRelTableIdxToScan = UINT32_MAX;
    uint32_t nextRelTableIdxToScan = 0;
};

class GenericScanRelTables : public ScanRelTable {
public:
    GenericScanRelTables(const DataPos& inNodeIDVectorPos, vector<DataPos> outputVectorsPos,
        unordered_map<table_id_t, unique_ptr<RelTableCollection>> relTableCollectionPerNodeTable,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : ScanRelTable{inNodeIDVectorPos, std::move(outputVectorsPos),
              PhysicalOperatorType::GENERIC_SCAN_REL_TABLES, std::move(child), id, paramsString},
          relTableCollectionPerNodeTable{std::move(relTableCollectionPerNodeTable)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        unordered_map<table_id_t, unique_ptr<RelTableCollection>> clonedCollections;
        for (auto& [tableID, propertyCollection] : relTableCollectionPerNodeTable) {
            clonedCollections.insert({tableID, propertyCollection->clone()});
        }
        return make_unique<GenericScanRelTables>(inNodeIDVectorPos, outputVectorsPos,
            std::move(clonedCollections), children[0]->clone(), id, paramsString);
    }

private:
    bool scanCurrentRelTableCollection();
    void initCurrentRelTableCollection(const nodeID_t& nodeID);

private:
    unordered_map<table_id_t, unique_ptr<RelTableCollection>> relTableCollectionPerNodeTable;
    RelTableCollection* currentRelTableCollection = nullptr;
};

} // namespace processor
} // namespace kuzu
