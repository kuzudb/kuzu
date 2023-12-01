#pragma once

#include "processor/operator/mask.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class BaseSemiMasker;

class SemiMaskerInfo {
    friend class BaseSemiMasker;

public:
    // Multiple maskers can point to the same SemiMask, thus we associate each masker with an idx
    // to indicate the execution sequence of its pipeline. Also, the maskerIdx is used as a flag to
    // indicate if a value in the mask is masked or not, as each masker will increment the selected
    // value in the mask by 1. More details are described in NodeTableSemiMask.
    using mask_with_idx = std::pair<NodeSemiMask*, uint8_t>;

    SemiMaskerInfo(const DataPos& keyPos,
        std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable)
        : keyPos{keyPos}, masksPerTable{std::move(masksPerTable)} {}
    SemiMaskerInfo(const SemiMaskerInfo& other)
        : keyPos{other.keyPos}, masksPerTable{other.masksPerTable} {}

    inline const std::vector<mask_with_idx>& getSingleTableMasks() const {
        KU_ASSERT(masksPerTable.size() == 1);
        return masksPerTable.begin()->second;
    }

    inline const std::vector<mask_with_idx>& getTableMasks(common::table_id_t tableID) const {
        KU_ASSERT(masksPerTable.contains(tableID));
        return masksPerTable.at(tableID);
    }

    inline std::unique_ptr<SemiMaskerInfo> copy() const {
        return std::make_unique<SemiMaskerInfo>(*this);
    }

private:
    DataPos keyPos;
    std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable;
};

class BaseSemiMasker : public PhysicalOperator {
protected:
    BaseSemiMasker(std::unique_ptr<SemiMaskerInfo> info, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SEMI_MASKER, std::move(child), id, paramsString},
          info{std::move(info)} {}

    void initGlobalStateInternal(ExecutionContext* context) override;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    std::unique_ptr<SemiMaskerInfo> info;
    common::ValueVector* keyVector;
};

class SingleTableSemiMasker : public BaseSemiMasker {
public:
    SingleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{std::move(info), std::move(child), id, paramsString} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<SingleTableSemiMasker>(
            info->copy(), children[0]->clone(), id, paramsString);
    }
};

class MultiTableSemiMasker : public BaseSemiMasker {
public:
    MultiTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{std::move(info), std::move(child), id, paramsString} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<MultiTableSemiMasker>(
            info->copy(), children[0]->clone(), id, paramsString);
    }
};

class PathSemiMasker : public BaseSemiMasker {
protected:
    PathSemiMasker(std::unique_ptr<SemiMaskerInfo> info, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : BaseSemiMasker{std::move(info), std::move(child), id, paramsString} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

protected:
    common::ValueVector* pathRelsVector;
    common::ValueVector* pathRelsSrcIDDataVector;
    common::ValueVector* pathRelsDstIDDataVector;
};

class PathSingleTableSemiMasker : public PathSemiMasker {
public:
    PathSingleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PathSemiMasker{std::move(info), std::move(child), id, paramsString} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathSingleTableSemiMasker>(
            info->copy(), children[0]->clone(), id, paramsString);
    }
};

class PathMultipleTableSemiMasker : public PathSemiMasker {
public:
    PathMultipleTableSemiMasker(std::unique_ptr<SemiMaskerInfo> info,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PathSemiMasker{std::move(info), std::move(child), id, paramsString} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathMultipleTableSemiMasker>(
            info->copy(), children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
