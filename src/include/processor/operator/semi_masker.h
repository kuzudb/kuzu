#pragma once

#include "common/mask.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class BaseSemiMasker;

// Multiple maskers can point to the same SemiMask, thus we associate each masker with an idx
// to indicate the execution sequence of its pipeline. Also, the maskerIdx is used as a flag to
// indicate if a value in the mask is masked or not, as each masker will increment the selected
// value in the mask by 1. More details are described in NodeTableSemiMask.
using mask_with_idx = std::pair<common::NodeSemiMask*, uint8_t>;

class SemiMaskerInfo {
    friend class BaseSemiMasker;

public:
    SemiMaskerInfo(const DataPos& keyPos,
        std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable)
        : keyPos{keyPos}, masksPerTable{std::move(masksPerTable)} {}
    SemiMaskerInfo(const SemiMaskerInfo& other)
        : keyPos{other.keyPos}, masksPerTable{other.masksPerTable} {}

    const std::vector<mask_with_idx>& getSingleTableMasks() const {
        KU_ASSERT(masksPerTable.size() == 1);
        return masksPerTable.begin()->second;
    }

    const std::vector<mask_with_idx>& getTableMasks(common::table_id_t tableID) const {
        KU_ASSERT(masksPerTable.contains(tableID));
        return masksPerTable.at(tableID);
    }

    std::unique_ptr<SemiMaskerInfo> copy() const { return std::make_unique<SemiMaskerInfo>(*this); }

private:
    DataPos keyPos;
    std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable;
};

struct SemiMaskerPrintInfo final : OPPrintInfo {
    std::vector<std::string> operatorNames;

    explicit SemiMaskerPrintInfo(std::vector<std::string> operatorNames)
        : operatorNames{std::move(operatorNames)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<SemiMaskerPrintInfo>(new SemiMaskerPrintInfo(*this));
    }

private:
    SemiMaskerPrintInfo(const SemiMaskerPrintInfo& other)
        : OPPrintInfo{other}, operatorNames{other.operatorNames} {}
};

class BaseSemiMasker : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SEMI_MASKER;

protected:
    BaseSemiMasker(std::unique_ptr<SemiMaskerInfo> info, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, keyVector{nullptr} {}

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    std::unique_ptr<SemiMaskerInfo> info;
    common::ValueVector* keyVector;
};

class SingleTableSemiMasker : public BaseSemiMasker {
public:
    SingleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<SingleTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy());
    }
};

class MultiTableSemiMasker : public BaseSemiMasker {
public:
    MultiTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<MultiTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy());
    }
};

class PathSemiMasker : public BaseSemiMasker {
protected:
    PathSemiMasker(std::unique_ptr<SemiMaskerInfo> info, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : BaseSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)},
          pathRelsVector{nullptr}, pathRelsSrcIDDataVector{nullptr},
          pathRelsDstIDDataVector{nullptr} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

protected:
    common::ValueVector* pathRelsVector;
    common::ValueVector* pathRelsSrcIDDataVector;
    common::ValueVector* pathRelsDstIDDataVector;
};

class PathSingleTableSemiMasker : public PathSemiMasker {
public:
    PathSingleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PathSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathSingleTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy());
    }
};

class PathMultipleTableSemiMasker : public PathSemiMasker {
public:
    PathMultipleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PathSemiMasker{std::move(info), std::move(child), id, std::move(printInfo)} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathMultipleTableSemiMasker>(info->copy(), children[0]->clone(), id,
            printInfo->copy());
    }
};

} // namespace processor
} // namespace kuzu
