#pragma once

#include "processor/operator/scan/scan_rel_table.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

class RelTableCollection {
public:
    RelTableCollection(std::vector<storage::DirectedRelTableData*> tables,
        std::vector<std::unique_ptr<storage::RelTableScanState>> tableScanStates)
        : tables{std::move(tables)}, tableScanStates{std::move(tableScanStates)} {}

    void resetState();
    inline uint32_t getNumTablesInCollection() { return tables.size(); }

    bool scan(common::ValueVector* inVector, const std::vector<common::ValueVector*>& outputVectors,
        transaction::Transaction* transaction);

    std::unique_ptr<RelTableCollection> clone() const;

private:
    std::vector<storage::DirectedRelTableData*> tables;
    std::vector<std::unique_ptr<storage::RelTableScanState>> tableScanStates;

    uint32_t currentRelTableIdxToScan = UINT32_MAX;
    uint32_t nextRelTableIdxToScan = 0;
};

class GenericScanRelTables : public ScanRelTable {
public:
    GenericScanRelTables(const DataPos& inNodeIDVectorPos, std::vector<DataPos> outputVectorsPos,
        std::unordered_map<common::table_id_t, std::unique_ptr<RelTableCollection>>
            relTableCollectionPerNodeTable,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : ScanRelTable{inNodeIDVectorPos, std::move(outputVectorsPos),
              PhysicalOperatorType::GENERIC_SCAN_REL_TABLES, std::move(child), id, paramsString},
          relTableCollectionPerNodeTable{std::move(relTableCollectionPerNodeTable)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        std::unordered_map<common::table_id_t, std::unique_ptr<RelTableCollection>>
            clonedCollections;
        for (auto& [tableID, propertyCollection] : relTableCollectionPerNodeTable) {
            clonedCollections.insert({tableID, propertyCollection->clone()});
        }
        return make_unique<GenericScanRelTables>(inNodeIDVectorPos, outputVectorsPos,
            std::move(clonedCollections), children[0]->clone(), id, paramsString);
    }

private:
    bool scanCurrentRelTableCollection();
    void initCurrentRelTableCollection(const common::nodeID_t& nodeID);

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<RelTableCollection>>
        relTableCollectionPerNodeTable;
    RelTableCollection* currentRelTableCollection = nullptr;
};

} // namespace processor
} // namespace kuzu
