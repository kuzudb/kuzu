#pragma once

#include "processor/operator/scan/scan_rel_table.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

class ScanRelTableLists : public ScanRelTable {
public:
    ScanRelTableLists(table_id_t boundNodeTableID, DirectedRelTableData* tableData,
        vector<uint32_t> propertyIds, const DataPos& inNodeIDVectorPos,
        vector<DataPos> outputVectorsPos, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : ScanRelTable{inNodeIDVectorPos, std::move(outputVectorsPos),
              PhysicalOperatorType::SCAN_REL_TABLE_LISTS, std::move(child), id, paramsString},
          tableData{tableData} {
        scanState = make_unique<RelTableScanState>(
            boundNodeTableID, std::move(propertyIds), RelTableDataType::LISTS);
    }

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelTableLists>(scanState->boundNodeTableID, tableData,
            scanState->propertyIds, inNodeIDVectorPos, outputVectorsPos, children[0]->clone(), id,
            paramsString);
    }

private:
    DirectedRelTableData* tableData;
    unique_ptr<RelTableScanState> scanState;
};

} // namespace processor
} // namespace kuzu
