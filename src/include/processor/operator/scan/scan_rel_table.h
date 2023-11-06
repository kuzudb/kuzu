#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

struct ScanRelTableInfo {
    storage::RelTable* table;
    common::RelDataDirection direction;
    std::vector<common::column_id_t> columnIDs;

    ScanRelTableInfo(storage::RelTable* table, common::RelDataDirection direction,
        std::vector<common::column_id_t> columnIDs)
        : table{table}, direction{direction}, columnIDs{std::move(columnIDs)} {}
    ScanRelTableInfo(const ScanRelTableInfo& other)
        : table{other.table}, direction{other.direction}, columnIDs{other.columnIDs} {}

    inline std::unique_ptr<ScanRelTableInfo> copy() const {
        return std::make_unique<ScanRelTableInfo>(*this);
    }
};

class ScanRelTable : public ScanTable {
public:
    ScanRelTable(std::unique_ptr<ScanRelTableInfo> info, const DataPos& inVectorPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : ScanRelTable{PhysicalOperatorType::SCAN_REL_TABLE, std::move(info), inVectorPos,
              std::move(outVectorsPos), std::move(child), id, paramsString} {
        scanState = std::make_unique<storage::RelDataReadState>(
            this->info->table->getTableDataFormat(this->info->direction));
    }
    virtual ~ScanRelTable() = default;

protected:
    ScanRelTable(PhysicalOperatorType operatorType, std::unique_ptr<ScanRelTableInfo> info,
        const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : ScanTable{operatorType, inVectorPos, std::move(outVectorsPos), std::move(child), id,
              paramsString},
          info{std::move(info)} {}

protected:
    std::unique_ptr<ScanRelTableInfo> info;
    std::unique_ptr<storage::RelDataReadState> scanState;
};

} // namespace processor
} // namespace kuzu
