#pragma once

#include <utility>

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

class ScanNodeTable final : public ScanTable {
public:
    ScanNodeTable(std::unique_ptr<ScanNodeTableInfo> info, const DataPos& inVectorPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : ScanNodeTable{PhysicalOperatorType::SCAN_NODE_TABLE, std::move(info), inVectorPos,
              std::move(outVectorsPos), std::move(child), id, paramsString} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* executionContext) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeTable>(info->copy(), nodeIDPos, outVectorsPos,
            children[0]->clone(), id, paramsString);
    }

protected:
    ScanNodeTable(PhysicalOperatorType operatorType, std::unique_ptr<ScanNodeTableInfo> info,
        const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : ScanTable{operatorType, inVectorPos, std::move(outVectorsPos), std::move(child), id,
              paramsString},
          info{std::move(info)} {}

private:
    std::unique_ptr<ScanNodeTableInfo> info;
    std::unique_ptr<storage::NodeTableReadState> readState;
};

} // namespace processor
} // namespace kuzu
