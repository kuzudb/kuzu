#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

struct ScanRelTalePosInfo {
    DataPos inNodeVectorPos;
    DataPos outNodeVectorPos;
    std::vector<DataPos> outVectorsPos;

    ScanRelTalePosInfo(const DataPos& inNodeVectorPos, const DataPos& outNodeVectorPos,
        std::vector<DataPos> outVectorsPos)
        : inNodeVectorPos{inNodeVectorPos}, outNodeVectorPos{outNodeVectorPos},
          outVectorsPos{std::move(outVectorsPos)} {}
    ScanRelTalePosInfo(const ScanRelTalePosInfo& other)
        : inNodeVectorPos{other.inNodeVectorPos}, outNodeVectorPos{other.outNodeVectorPos},
          outVectorsPos{other.outVectorsPos} {}

    inline std::unique_ptr<ScanRelTalePosInfo> copy() const {
        return std::make_unique<ScanRelTalePosInfo>(*this);
    }
};

struct RelTableScanInfo {
    storage::RelTableDataType relTableDataType;
    storage::DirectedRelTableData* tableData;
    storage::RelStatistics* relStats;
    std::vector<common::property_id_t> propertyIds;

    RelTableScanInfo(storage::RelTableDataType relTableDataType,
        storage::DirectedRelTableData* tableData, storage::RelStatistics* relStats,
        std::vector<common::property_id_t> propertyIds)
        : relTableDataType{relTableDataType}, tableData{tableData}, relStats{relStats},
          propertyIds{std::move(propertyIds)} {}
    RelTableScanInfo(const RelTableScanInfo& other)
        : relTableDataType{other.relTableDataType}, tableData{other.tableData},
          relStats{other.relStats}, propertyIds{other.propertyIds} {}

    inline std::unique_ptr<RelTableScanInfo> copy() const {
        return std::make_unique<RelTableScanInfo>(*this);
    }
};

class ScanRelTable : public PhysicalOperator {
protected:
    ScanRelTable(std::unique_ptr<ScanRelTalePosInfo> posInfo, PhysicalOperatorType operatorType,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString}, posInfo{std::move(
                                                                                  posInfo)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    std::unique_ptr<ScanRelTalePosInfo> posInfo;

    common::ValueVector* inNodeVector;
    common::ValueVector* outNodeVector;
    std::vector<common::ValueVector*> outVectors;
};

} // namespace processor
} // namespace kuzu
