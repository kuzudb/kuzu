#pragma once

#include <unordered_map>

#include "common/data_chunk/data_chunk_collection.h"
#include "common/enums/rel_multiplicity.h"
#include "common/enums/table_type.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace storage {

using offset_to_row_idx_t = std::unordered_map<common::offset_t, common::row_idx_t>;
using offset_to_row_idx_vec_t =
    std::unordered_map<common::offset_t, std::vector<common::row_idx_t>>;
using offset_set_t = std::unordered_set<common::offset_t>;

static constexpr common::column_id_t NBR_ID_COLUMN_ID = 0;
static constexpr common::column_id_t REL_ID_COLUMN_ID = 1;

struct LocalVectorCollection {
    std::vector<common::ValueVector*> vectors;

    static LocalVectorCollection empty() { return LocalVectorCollection{}; }

    inline bool isEmpty() const { return vectors.empty(); }
    inline void appendVector(common::ValueVector* vector) { vectors.push_back(vector); }
    inline common::ValueVector* getLocalVector(common::row_idx_t rowIdx) const {
        auto vectorIdx = rowIdx >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
        KU_ASSERT(vectorIdx < vectors.size());
        return vectors[vectorIdx];
    }

    LocalVectorCollection getStructChildVectorCollection(common::struct_field_idx_t idx) const;
};

class LocalDataChunkCollection {
public:
    LocalDataChunkCollection(MemoryManager* mm, std::vector<common::LogicalType> dataTypes)
        : dataChunkCollection{mm}, mm{mm}, dataTypes{std::move(dataTypes)}, numRows{0} {}

    inline common::row_idx_t getRowIdxFromOffset(common::offset_t offset) {
        KU_ASSERT(offsetToRowIdx.contains(offset));
        return offsetToRowIdx.at(offset);
    }
    inline std::vector<common::row_idx_t>& getRelOffsetsFromSrcOffset(common::offset_t srcOffset) {
        KU_ASSERT(srcNodeOffsetToRelOffsets.contains(srcOffset));
        return srcNodeOffsetToRelOffsets.at(srcOffset);
    }
    inline bool hasOffset(common::offset_t offset) const { return offsetToRowIdx.contains(offset); }
    inline bool hasRelOffsetsFromSrcOffset(common::offset_t srcOffset) const {
        return srcNodeOffsetToRelOffsets.contains(srcOffset);
    }
    inline uint64_t getNumRelsFromSrcOffset(common::offset_t srcOffset) const {
        return srcNodeOffsetToRelOffsets.at(srcOffset).size();
    }
    inline const offset_to_row_idx_vec_t& getSrcNodeOffsetToRelOffsets() const {
        return srcNodeOffsetToRelOffsets;
    }
    inline const offset_to_row_idx_t& getOffsetToRowIdx() const { return offsetToRowIdx; }

    bool isEmpty() const { return offsetToRowIdx.empty() && srcNodeOffsetToRelOffsets.empty(); }
    void readValueAtRowIdx(common::row_idx_t rowIdx, common::column_id_t columnID,
        common::ValueVector* outputVector, common::sel_t posInOutputVector);
    bool read(common::offset_t offset, common::column_id_t columnID,
        common::ValueVector* outputVector, common::sel_t posInOutputVector);

    inline void append(common::offset_t offset, std::vector<common::ValueVector*> vectors) {
        offsetToRowIdx[offset] = appendToDataChunkCollection(vectors);
    }
    // Only used for rel tables. Should be moved out later.
    inline void append(common::offset_t nodeOffset, common::offset_t relOffset,
        std::vector<common::ValueVector*> vectors) {
        append(relOffset, vectors);
        srcNodeOffsetToRelOffsets[nodeOffset].push_back(relOffset);
    }
    void update(
        common::offset_t offset, common::column_id_t columnID, common::ValueVector* propertyVector);
    void remove(common::offset_t offset) {
        if (offsetToRowIdx.contains(offset)) {
            offsetToRowIdx.erase(offset);
        }
    }
    // Only used for rel tables. Should be moved out later.
    void remove(common::offset_t srcNodeOffset, common::offset_t relOffset);

    inline LocalVectorCollection getLocalChunk(common::column_id_t columnID) {
        LocalVectorCollection localVectorCollection;
        for (auto& chunk : dataChunkCollection.getChunksUnsafe()) {
            localVectorCollection.appendVector(chunk.getValueVector(columnID).get());
        }
        return localVectorCollection;
    }

private:
    common::row_idx_t appendToDataChunkCollection(std::vector<common::ValueVector*> vectors);
    common::DataChunk createNewDataChunk();

private:
    common::DataChunkCollection dataChunkCollection;
    // The offset here can either be nodeOffset ( for node table) or relOffset (for rel table).
    offset_to_row_idx_t offsetToRowIdx;
    storage::MemoryManager* mm;
    std::vector<common::LogicalType> dataTypes;
    common::row_idx_t numRows;

    // Only used for rel tables. Should be moved out later.
    offset_to_row_idx_vec_t srcNodeOffsetToRelOffsets;
};

class LocalDeletionInfo {
public:
    bool isEmpty() const { return deletedOffsets.empty() && srcNodeOffsetToRelOffsetVec.empty(); }
    bool isEmpty(common::offset_t srcOffset) const {
        return !srcNodeOffsetToRelOffsetVec.contains(srcOffset) ||
               srcNodeOffsetToRelOffsetVec.at(srcOffset).empty();
    }
    bool containsOffset(common::offset_t offset) { return deletedOffsets.contains(offset); }
    bool deleteOffset(common::offset_t offset) {
        if (deletedOffsets.contains(offset)) {
            return false;
        }
        deletedOffsets.insert(offset);
        return true;
    }

    // For rel tables only.
    void deleteRelAux(common::offset_t srcNodeOffset, common::offset_t relOffset) {
        srcNodeOffsetToRelOffsetVec[srcNodeOffset].push_back(relOffset);
    }
    const offset_to_row_idx_vec_t& getSrcNodeOffsetToRelOffsetVec() const {
        return srcNodeOffsetToRelOffsetVec;
    }
    uint64_t getNumDeletedRelsFromSrcOffset(common::offset_t srcOffset) const {
        return srcNodeOffsetToRelOffsetVec.contains(srcOffset) ?
                   srcNodeOffsetToRelOffsetVec.at(srcOffset).size() :
                   0;
    }

private:
    // The offset here can either be nodeOffset ( for node table) or relOffset (for rel table).
    offset_set_t deletedOffsets;

    // Only used for rel tables. Should be moved out later.
    offset_to_row_idx_vec_t srcNodeOffsetToRelOffsetVec;
};

class LocalNodeGroup {
public:
    LocalNodeGroup(common::offset_t nodeGroupStartOffset,
        std::vector<common::LogicalType*> dataTypes, MemoryManager* mm);
    virtual ~LocalNodeGroup() = default;

    virtual bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> propertyVectors) = 0;
    virtual bool update(std::vector<common::ValueVector*> nodeIDVectors,
        common::column_id_t columnID, common::ValueVector* propertyVector) = 0;
    virtual bool delete_(common::ValueVector* IDVector, common::ValueVector* extraVector) = 0;

    LocalDataChunkCollection& getUpdateChunks(common::column_id_t columnID) {
        KU_ASSERT(columnID < updateChunks.size());
        return updateChunks[columnID];
    }
    LocalDataChunkCollection& getInsesrtChunks() { return insertChunks; }

    bool hasUpdatesOrDeletions() const;

protected:
    common::offset_t nodeGroupStartOffset;
    storage::MemoryManager* mm;

    LocalDataChunkCollection insertChunks;
    LocalDeletionInfo deleteInfo;
    std::vector<LocalDataChunkCollection> updateChunks;
};

class LocalTableData {
    friend class NodeTableData;

public:
    LocalTableData(std::vector<common::LogicalType*> dataTypes, MemoryManager* mm)
        : dataTypes{std::move(dataTypes)}, mm{mm} {}
    virtual ~LocalTableData() = default;

    inline void clear() { nodeGroups.clear(); }

    bool insert(std::vector<common::ValueVector*> nodeIDVectors,
        std::vector<common::ValueVector*> propertyVectors);
    bool update(std::vector<common::ValueVector*> nodeIDVectors, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    bool delete_(common::ValueVector* nodeIDVector, common::ValueVector* extraVector = nullptr);

protected:
    virtual LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) = 0;

protected:
    std::vector<common::LogicalType*> dataTypes;
    MemoryManager* mm;

    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>> nodeGroups;
};

class Column;
class LocalTable {
public:
    explicit LocalTable(common::TableType tableType) : tableType{tableType} {};

    LocalTableData* getOrCreateLocalTableData(const std::vector<std::unique_ptr<Column>>& columns,
        MemoryManager* mm, common::vector_idx_t dataIdx, common::RelMultiplicity multiplicity);
    inline LocalTableData* getLocalTableData(common::vector_idx_t dataIdx) {
        KU_ASSERT(dataIdx < localTableDataCollection.size());
        return localTableDataCollection[dataIdx].get();
    }

private:
    common::TableType tableType;
    // For a node table, it should only contain one LocalTableData, while a rel table should contain
    // two, one for each direction.
    std::vector<std::unique_ptr<LocalTableData>> localTableDataCollection;
};

} // namespace storage
} // namespace kuzu
