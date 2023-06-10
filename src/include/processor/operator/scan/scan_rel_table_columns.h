#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/scan/scan_rel_table.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

class ScanRelTableColumns : public ScanRelTable, public SelVectorOverWriter {
public:
    ScanRelTableColumns(storage::DirectedRelTableData* tableData, storage::RelStatistics* relStats,
        std::vector<uint32_t> propertyIds, const DataPos& inNodeIDVectorPos,
        std::vector<DataPos> outputVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : ScanRelTable{inNodeIDVectorPos, std::move(outputVectorsPos),
              PhysicalOperatorType::SCAN_REL_TABLE_COLUMNS, std::move(child), id, paramsString},
          tableData{tableData} {
        scanState = std::make_unique<storage::RelTableScanState>(
            relStats, std::move(propertyIds), storage::RelTableDataType::COLUMNS);
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanRelTableColumns>(tableData, scanState->relStats,
            scanState->propertyIds, inNodeIDVectorPos, outputVectorsPos, children[0]->clone(), id,
            paramsString);
    }

private:
    storage::DirectedRelTableData* tableData;
    std::unique_ptr<storage::RelTableScanState> scanState;
};

} // namespace processor
} // namespace kuzu
