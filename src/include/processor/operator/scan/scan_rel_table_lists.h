#pragma once

#include "processor/operator/scan/scan_rel_table.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

class ScanRelTableLists : public ScanRelTable {
public:
    ScanRelTableLists(storage::DirectedRelTableData* tableData, storage::RelStatistics* relStats,
        std::vector<uint32_t> propertyIds, const DataPos& inNodeIDVectorPos,
        std::vector<DataPos> outputVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : ScanRelTable{inNodeIDVectorPos, std::move(outputVectorsPos),
              PhysicalOperatorType::SCAN_REL_TABLE_LISTS, std::move(child), id, paramsString},
          tableData{tableData} {
        scanState = std::make_unique<storage::RelTableScanState>(
            relStats, std::move(propertyIds), storage::RelTableDataType::LISTS);
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelTableLists>(tableData, scanState->relStats,
            scanState->propertyIds, inNodeIDVectorPos, outputVectorsPos, children[0]->clone(), id,
            paramsString);
    }

private:
    storage::DirectedRelTableData* tableData;
    std::unique_ptr<storage::RelTableScanState> scanState;
};

} // namespace processor
} // namespace kuzu
