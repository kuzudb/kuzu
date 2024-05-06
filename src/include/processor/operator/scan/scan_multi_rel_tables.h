#pragma once

#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

class RelTableCollectionScanner {
    friend class ScanMultiRelTable;

public:
    explicit RelTableCollectionScanner(std::vector<std::unique_ptr<ScanRelTableInfo>> scanInfos)
        : scanInfos{std::move(scanInfos)} {}

    void resetState() {
        currentTableIdx = 0;
        nextTableIdx = 0;
    }

    bool scan(const common::SelectionVector& selVector, transaction::Transaction* transaction);

    std::unique_ptr<RelTableCollectionScanner> clone() const;

private:
    std::vector<std::unique_ptr<ScanRelTableInfo>> scanInfos;
    std::vector<std::unique_ptr<storage::RelTableReadState>> readStates;
    uint32_t currentTableIdx = UINT32_MAX;
    uint32_t nextTableIdx = 0;
};

class ScanMultiRelTable : public ScanTable {
    using node_table_id_scanner_map_t =
        std::unordered_map<common::table_id_t, std::unique_ptr<RelTableCollectionScanner>>;

public:
    ScanMultiRelTable(node_table_id_scanner_map_t scannerPerNodeTable, const DataPos& inVectorPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : ScanTable{PhysicalOperatorType::SCAN_MULTI_REL_TABLES, inVectorPos,
              std::move(outVectorsPos), std::move(child), id, paramsString},
          scannerPerNodeTable{std::move(scannerPerNodeTable)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    void resetState();
    void initCurrentScanner(const common::nodeID_t& nodeID);

private:
    node_table_id_scanner_map_t scannerPerNodeTable;
    RelTableCollectionScanner* currentScanner = nullptr;
};

} // namespace processor
} // namespace kuzu
