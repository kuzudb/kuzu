#pragma once

#include "common/copy_constructors.h"
#include "common/enums/rel_direction.h"
#include "common/vector/value_vector.h"
#include "storage/local_storage/local_table.h"

namespace kuzu {
namespace storage {

static constexpr common::column_id_t LOCAL_NBR_ID_COLUMN_ID = 0;
static constexpr common::column_id_t LOCAL_REL_ID_COLUMN_ID = 1;

class LocalRelNG final : public LocalNodeGroup {
    friend class RelTableData;

public:
    LocalRelNG(common::offset_t nodeGroupStartOffset, std::vector<common::LogicalType> dataTypes);
    DELETE_COPY_DEFAULT_MOVE(LocalRelNG);

    common::row_idx_t scanCSR(common::offset_t srcOffset, common::offset_t posToReadForOffset,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVector);
    // For CSR, we need to apply updates and deletions here, while insertions are handled by
    // `scanCSR`.
    void applyLocalChangesToScannedVectors(common::offset_t srcOffset,
        const std::vector<common::column_id_t>& columnIDs, common::ValueVector* relIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> vectors) override;
    bool update(std::vector<common::ValueVector*> nodeIDVectors, common::column_id_t columnID,
        common::ValueVector* propertyVector) override;
    bool delete_(common::ValueVector* srcNodeVector, common::ValueVector* relIDVector) override;

    common::offset_t getNumInsertedRels(common::offset_t srcOffset) const;
    void getChangesPerCSRSegment(std::vector<int64_t>& sizeChangesPerSegment,
        std::vector<bool>& hasChangesPerSegment);

private:
    static common::vector_idx_t getSegmentIdx(common::offset_t offset) {
        return offset >> common::StorageConstants::CSR_SEGMENT_SIZE_LOG2;
    }

    void applyCSRUpdates(common::column_id_t columnID, common::ValueVector* relIDVector,
        common::ValueVector* outputVector);
    void applyCSRDeletions(common::offset_t srcOffsetInChunk, common::ValueVector* relIDVector);
};

class LocalRelTableData final : public LocalTableData {
    friend class RelTableData;

public:
    explicit LocalRelTableData(std::vector<common::LogicalType> dataTypes)
        : LocalTableData{std::move(dataTypes)} {}

private:
    LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) override;
};

class LocalRelTable final : public LocalTable {
public:
    explicit LocalRelTable(Table& table);

    bool insert(TableInsertState& insertState) override;
    bool update(TableUpdateState& updateState) override;
    bool delete_(TableDeleteState& deleteState) override;

    LocalRelTableData* getTableData(common::RelDataDirection direction) {
        KU_ASSERT(localTableDataCollection.size() == 2);
        return common::ku_dynamic_cast<LocalTableData*, LocalRelTableData*>(
            direction == common::RelDataDirection::FWD ? localTableDataCollection[0].get() :
                                                         localTableDataCollection[1].get());
    }
};

} // namespace storage
} // namespace kuzu
