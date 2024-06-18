#pragma once

#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

struct DirectionInfo {
    bool extendFromSource;
    DataPos directionPos;

    DirectionInfo() : extendFromSource{false}, directionPos{DataPos::getInvalidPos()} {}
    EXPLICIT_COPY_DEFAULT_MOVE(DirectionInfo);

    bool needFlip(common::RelDataDirection relDataDirection) const;

private:
    DirectionInfo(const DirectionInfo& other)
        : extendFromSource{other.extendFromSource}, directionPos{other.directionPos} {}
};

class RelTableCollectionScanner {
    friend class ScanMultiRelTable;

public:
    explicit RelTableCollectionScanner(std::vector<ScanRelTableInfo> relInfos)
        : relInfos{std::move(relInfos)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(RelTableCollectionScanner);

    bool empty() const { return relInfos.empty(); }

    void resetState() {
        currentTableIdx = 0;
        nextTableIdx = 0;
    }

    bool scan(const common::SelectionVector& selVector, transaction::Transaction* transaction);

private:
    RelTableCollectionScanner(const RelTableCollectionScanner& other)
        : relInfos{copyVector(other.relInfos)} {}

private:
    std::vector<ScanRelTableInfo> relInfos;
    std::vector<bool> directionValues;
    common::ValueVector* directionVector = nullptr;
    common::idx_t currentTableIdx = common::INVALID_IDX;
    uint32_t nextTableIdx = 0;
};

class ScanMultiRelTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SCAN_REL_TABLE;

public:
    ScanMultiRelTable(ScanTableInfo info, DirectionInfo directionInfo,
        common::table_id_map_t<RelTableCollectionScanner> scanners,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), std::move(child), id, std::move(printInfo)},
          directionInfo{std::move(directionInfo)}, scanners{std::move(scanners)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    void resetState();
    void initCurrentScanner(const common::nodeID_t& nodeID);

private:
    DirectionInfo directionInfo;
    common::table_id_map_t<RelTableCollectionScanner> scanners;
    RelTableCollectionScanner* currentScanner = nullptr;
};

} // namespace processor
} // namespace kuzu
