#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct ScanNodeTableInfo {
    storage::NodeTable* table;
    std::vector<common::column_id_t> columnIDs;

    ScanNodeTableInfo(storage::NodeTable* table, std::vector<common::column_id_t> columnIDs)
        : table{table}, columnIDs{std::move(columnIDs)} {}
    ScanNodeTableInfo(const ScanNodeTableInfo& other)
        : table{other.table}, columnIDs{other.columnIDs} {}

    inline std::unique_ptr<ScanNodeTableInfo> copy() const {
        return std::make_unique<ScanNodeTableInfo>(*this);
    }
};

class ScanSingleNodeTable : public ScanTable {
public:
    ScanSingleNodeTable(std::unique_ptr<ScanNodeTableInfo> info, const DataPos& inVectorPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : ScanSingleNodeTable{PhysicalOperatorType::SCAN_NODE_TABLE, std::move(info), inVectorPos,
              outVectorsPos, std::move(child), id, paramsString} {}

    inline void initLocalStateInternal(
        ResultSet* resultSet, ExecutionContext* executionContext) final {
        ScanTable::initLocalStateInternal(resultSet, executionContext);
        readState = std::make_unique<storage::TableReadState>();
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanSingleNodeTable>(
            info->copy(), inVectorPos, outVectorsPos, children[0]->clone(), id, paramsString);
    }

protected:
    ScanSingleNodeTable(PhysicalOperatorType operatorType, std::unique_ptr<ScanNodeTableInfo> info,
        const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : ScanTable{operatorType, inVectorPos, std::move(outVectorsPos), std::move(child), id,
              paramsString},
          info{std::move(info)} {}

private:
    std::unique_ptr<ScanNodeTableInfo> info;
    std::unique_ptr<storage::TableReadState> readState;
};

} // namespace processor
} // namespace kuzu
