#pragma once

#include "common/column_data_format.h"
#include "common/enums/rel_direction.h"
#include "storage/local_table.h"

namespace kuzu {
namespace storage {

using csr_offset_to_row_idx_t =
    std::map<common::offset_t, std::map<common::offset_t, common::row_idx_t>>;

class RelNGInfo {
public:
    virtual ~RelNGInfo() = default;

    virtual void insert(common::offset_t srcNodeOffset, common::offset_t relID,
        common::row_idx_t rowIdx, const std::vector<common::row_idx_t>& columnsRowIdx) = 0;
    virtual void update(common::offset_t srcNodeOffset, common::offset_t relID,
        common::row_idx_t rowIdx, common::column_id_t columnID) = 0;
    virtual bool delete_(common::offset_t srcNodeOffset, common::offset_t relID) = 0;
};

class RegularRelNGInfo final : public RelNGInfo {
public:
    RegularRelNGInfo(common::column_id_t numColumns) : RelNGInfo() {
        insertInfoPerColumn.resize(numColumns);
        updateInfoPerColumn.resize(numColumns);
    }

    void insert(common::offset_t srcNodeOffset, common::offset_t /*relID*/,
        common::row_idx_t adjRowIdx, const std::vector<common::row_idx_t>& columnsRowIdx) {
        KU_ASSERT(columnsRowIdx.size() == insertInfoPerColumn.size());
        adjInsertInfo[srcNodeOffset] = adjRowIdx;
        for (auto i = 0u; i < columnsRowIdx.size(); i++) {
            insertInfoPerColumn[i][srcNodeOffset] = columnsRowIdx[i];
        }
    }

    void update(common::offset_t srcNodeOffset, common::offset_t /*relID*/,
        common::row_idx_t rowIdx, common::column_id_t columnID) {
        KU_ASSERT(columnID < updateInfoPerColumn.size());
        if (insertInfoPerColumn[columnID].contains(srcNodeOffset)) {
            // Update newly inserted rel.
            insertInfoPerColumn[columnID][srcNodeOffset] = rowIdx;
        } else {
            updateInfoPerColumn[columnID][srcNodeOffset] = rowIdx;
        }
    }

    bool delete_(common::offset_t srcNodeOffset, common::offset_t /*relID*/) {
        if (adjInsertInfo.contains(srcNodeOffset)) {
            // Delete newly inserted rel.
            adjInsertInfo.erase(srcNodeOffset);
            for (auto i = 0u; i < insertInfoPerColumn.size(); i++) {
                insertInfoPerColumn[i].erase(srcNodeOffset);
            }
        } else {
            if (deleteInfo.contains(srcNodeOffset)) {
                return false;
            }
            deleteInfo.insert(srcNodeOffset);
        }
        return true;
    }

    offset_to_row_idx_t adjInsertInfo;
    std::unordered_set<common::offset_t> deleteInfo; // delete node offsets.
    std::vector<offset_to_row_idx_t> updateInfoPerColumn;
    std::vector<offset_to_row_idx_t> insertInfoPerColumn;
};

class CSRRelNGInfo final : public RelNGInfo {
public:
    CSRRelNGInfo(common::column_id_t numColumns) : RelNGInfo() {
        updateInfoPerColumn.resize(numColumns);
        insertInfoPerColumn.resize(numColumns);
    }

    void insert(common::offset_t srcNodeOffset, common::offset_t relID, common::row_idx_t adjRowIdx,
        const std::vector<common::row_idx_t>& columnsRowIdx) {
        if (adjInsertInfo.contains(srcNodeOffset)) {
            KU_ASSERT(!adjInsertInfo[srcNodeOffset].contains(relID));
            adjInsertInfo[srcNodeOffset][relID] = adjRowIdx;
        } else {
            adjInsertInfo[srcNodeOffset] = {{relID, adjRowIdx}};
        }
        KU_ASSERT(columnsRowIdx.size() == insertInfoPerColumn.size());
        for (auto i = 0u; i < columnsRowIdx.size(); i++) {
            if (insertInfoPerColumn[i].contains(srcNodeOffset)) {
                KU_ASSERT(!insertInfoPerColumn[i][srcNodeOffset].contains(relID));
                insertInfoPerColumn[i][srcNodeOffset][relID] = columnsRowIdx[i];
            } else {
                insertInfoPerColumn[i][srcNodeOffset] = {{relID, columnsRowIdx[i]}};
            }
        }
    }

    void update(common::offset_t srcNodeOffset, common::offset_t relID, common::row_idx_t rowIdx,
        common::column_id_t columnID) {
        KU_ASSERT(columnID < updateInfoPerColumn.size());
        if (insertInfoPerColumn[columnID].contains(srcNodeOffset) &&
            insertInfoPerColumn[columnID][srcNodeOffset].contains(relID)) {
            // Update newly inserted rel.
            insertInfoPerColumn[columnID][srcNodeOffset][relID] = rowIdx;
        } else {
            if (updateInfoPerColumn[columnID].contains(srcNodeOffset)) {
                KU_ASSERT(!updateInfoPerColumn[columnID][srcNodeOffset].contains(relID));
                updateInfoPerColumn[columnID].at(srcNodeOffset)[relID] = rowIdx;
            } else {
                updateInfoPerColumn[columnID][srcNodeOffset][relID] = rowIdx;
            }
        }
    }

    bool delete_(common::offset_t srcNodeOffset, common::offset_t relID) {
        if (adjInsertInfo.contains(srcNodeOffset) && adjInsertInfo[srcNodeOffset].contains(relID)) {
            // Delete newly inserted rel.
            adjInsertInfo[srcNodeOffset].erase(relID);
            if (adjInsertInfo[srcNodeOffset].empty()) {
                adjInsertInfo.erase(srcNodeOffset);
            }
            for (auto i = 0u; i < insertInfoPerColumn.size(); i++) {
                if (insertInfoPerColumn[i].contains(srcNodeOffset) &&
                    insertInfoPerColumn[i][srcNodeOffset].contains(relID)) {
                    insertInfoPerColumn[i][srcNodeOffset].erase(relID);
                    if (insertInfoPerColumn[i][srcNodeOffset].empty()) {
                        insertInfoPerColumn[i].erase(srcNodeOffset);
                    }
                }
            }
        } else {
            if (deleteInfo.contains(srcNodeOffset)) {
                if (deleteInfo.at(srcNodeOffset).contains(relID)) {
                    return false;
                }
                deleteInfo[srcNodeOffset].insert(relID);
            } else {
                deleteInfo[srcNodeOffset] = {relID};
            }
        }
        return true;
    }

    csr_offset_to_row_idx_t adjInsertInfo;
    std::vector<csr_offset_to_row_idx_t> updateInfoPerColumn;
    std::vector<csr_offset_to_row_idx_t> insertInfoPerColumn;
    std::map<common::offset_t, std::unordered_set<common::offset_t>>
        deleteInfo; // srcNodeOffset -> relIDs
};

class LocalRelNG final : public LocalNodeGroup {
public:
    LocalRelNG(common::ColumnDataFormat dataFormat,
        const std::vector<std::unique_ptr<common::LogicalType>>& dataTypes, MemoryManager* mm)
        : LocalNodeGroup(dataTypes, mm) {
        adjColumn = std::make_unique<LocalVectorCollection>(
            std::make_unique<common::LogicalType>(common::LogicalTypeID::INTERNAL_ID), mm);
        if (dataFormat == common::ColumnDataFormat::REGULAR) {
            relNGInfo = std::make_unique<RegularRelNGInfo>(dataTypes.size());
        } else {
            relNGInfo = std::make_unique<CSRRelNGInfo>(dataTypes.size());
        }
    }

    void insert(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    bool delete_(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector);

    inline LocalVectorCollection* getAdjColumn() { return adjColumn.get(); }
    inline RelNGInfo* getRelNodeGroupInfo() { return relNGInfo.get(); }

private:
    std::unique_ptr<LocalVectorCollection> adjColumn;
    std::unique_ptr<RelNGInfo> relNGInfo;
};

class LocalRelTableData {
    friend class RelTableData;

public:
    LocalRelTableData(common::ColumnDataFormat dataFormat,
        std::vector<std::unique_ptr<common::LogicalType>> dataTypes, MemoryManager* mm)
        : dataFormat{dataFormat}, dataTypes{std::move(dataTypes)}, mm{mm} {}

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void insert(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    bool delete_(common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector);

    inline void clear() { nodeGroups.clear(); }

private:
    common::node_group_idx_t initializeLocalNodeGroup(common::ValueVector* nodeIDVector);

private:
    common::ColumnDataFormat dataFormat;
    std::vector<std::unique_ptr<common::LogicalType>> dataTypes;
    MemoryManager* mm;
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalRelNG>> nodeGroups;
};

class LocalRelTable final : public LocalTable {
    friend class RelTableData;

public:
    LocalRelTable(common::table_id_t tableID,
        std::vector<std::unique_ptr<common::LogicalType>> dataTypes, MemoryManager* mm)
        : LocalTable{tableID, std::move(dataTypes), mm} {}

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

    inline void clear() {
        fwdRelTableData->clear();
        bwdRelTableData->clear();
    }

    LocalRelTableData* getOrCreateRelTableData(
        common::RelDataDirection direction, common::ColumnDataFormat dataFormat);

    inline LocalRelTableData* getRelTableData(common::RelDataDirection direction) {
        return direction == common::RelDataDirection::FWD ? fwdRelTableData.get() :
                                                            bwdRelTableData.get();
    }

private:
    inline static std::vector<std::unique_ptr<common::LogicalType>> copyTypes(
        const std::vector<std::unique_ptr<common::LogicalType>>& dataTypes) {
        std::vector<std::unique_ptr<common::LogicalType>> types;
        types.reserve(dataTypes.size());
        for (auto& type : dataTypes) {
            types.push_back(type->copy());
        }
        return types;
    }

private:
    std::unique_ptr<LocalRelTableData> fwdRelTableData;
    std::unique_ptr<LocalRelTableData> bwdRelTableData;
};

} // namespace storage
} // namespace kuzu
