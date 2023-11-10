#pragma once

#include <map>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace storage {
class TableData;

// TODO(Guodong): Instead of using ValueVector, we should switch to ColumnChunk.
// This class is used to store a chunk of local changes to a column in a node group.
// Values are stored inside `vector`.
class LocalVector {
public:
    LocalVector(const common::LogicalType& dataType, MemoryManager* mm) : numValues{0} {
        vector = std::make_unique<common::ValueVector>(dataType, mm);
        vector->state = std::make_unique<common::DataChunkState>();
        vector->state->selVector->resetSelectorToValuePosBufferWithSize(1);
    }

    void read(common::sel_t offsetInLocalVector, common::ValueVector* resultVector,
        common::sel_t offsetInResultVector);
    void append(common::ValueVector* valueVector);

    inline common::ValueVector* getVector() { return vector.get(); }
    inline bool isFull() { return numValues == common::DEFAULT_VECTOR_CAPACITY; }

private:
    std::unique_ptr<common::ValueVector> vector;
    common::sel_t numValues;
};

// This class is used to store local changes of a column in a node group.
// It consists of a collection of LocalVector, each of which is a chunk of the local changes.
// By default, the size of each vector (chunk) is DEFAULT_VECTOR_CAPACITY, and the collection
// contains 64 vectors (chunks).
class LocalVectorCollection {
public:
    LocalVectorCollection(const common::LogicalType* dataType, MemoryManager* mm)
        : dataType{dataType}, mm{mm}, numRows{0} {}

    void read(common::row_idx_t rowIdx, common::ValueVector* outputVector,
        common::sel_t posInOutputVector);
    void insert(common::ValueVector* nodeIDVector, common::ValueVector* propertyVectors);
    void update(common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    inline void delete_(common::offset_t nodeOffset) {
        insertInfo.erase(nodeOffset);
        updateInfo.erase(nodeOffset);
    }
    inline std::map<common::offset_t, common::row_idx_t>& getInsertInfoRef() { return insertInfo; }
    inline std::map<common::offset_t, common::row_idx_t>& getUpdateInfoRef() { return updateInfo; }
    inline uint64_t getNumRows() { return numRows; }
    inline LocalVector* getLocalVector(common::row_idx_t rowIdx) {
        auto vectorIdx = rowIdx >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
        KU_ASSERT(vectorIdx < vectors.size());
        return vectors[vectorIdx].get();
    }

    common::row_idx_t getRowIdx(common::offset_t nodeOffset);

private:
    void prepareAppend();
    void append(common::ValueVector* vector);

private:
    const common::LogicalType* dataType;
    MemoryManager* mm;
    std::vector<std::unique_ptr<LocalVector>> vectors;
    common::row_idx_t numRows;
    // TODO: Do we need to differentiate between insert and update?
    // New nodes to be inserted into the persistent storage.
    std::map<common::offset_t, common::row_idx_t> insertInfo;
    // Nodes in the persistent storage to be updated.
    std::map<common::offset_t, common::row_idx_t> updateInfo;
};

class LocalNodeGroup {
    friend class NodeTableData;

public:
    LocalNodeGroup(
        const std::vector<std::unique_ptr<common::LogicalType>>& dataTypes, MemoryManager* mm) {
        columns.resize(dataTypes.size());
        for (auto i = 0u; i < dataTypes.size(); ++i) {
            // To avoid unnecessary memory consumption, we chunk local changes of each column in the
            // node group into chunks of size DEFAULT_VECTOR_CAPACITY.
            columns[i] = std::make_unique<LocalVectorCollection>(dataTypes[i].get(), mm);
        }
    }

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::offset_t nodeOffset, common::column_id_t columnID,
        common::ValueVector* outputVector, common::sel_t posInOutputVector);
    void insert(common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* nodeIDVector, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    void delete_(common::ValueVector* nodeIDVector);

    inline LocalVectorCollection* getLocalColumnChunk(common::column_id_t columnID) {
        return columns[columnID].get();
    }

private:
    inline common::row_idx_t getRowIdx(common::column_id_t columnID, common::offset_t nodeOffset) {
        KU_ASSERT(columnID < columns.size());
        return columns[columnID]->getRowIdx(nodeOffset);
    }

private:
    std::vector<std::unique_ptr<LocalVectorCollection>> columns;
};

class LocalTable {
    friend class NodeTableData;

public:
    explicit LocalTable(common::table_id_t tableID,
        std::vector<std::unique_ptr<common::LogicalType>> dataTypes, MemoryManager* mm)
        : tableID{tableID}, dataTypes{std::move(dataTypes)}, mm{mm} {};

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void insert(common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(common::ValueVector* nodeIDVector, common::column_id_t columnID,
        common::ValueVector* propertyVector);
    void delete_(common::ValueVector* nodeIDVector);

    inline void clear() { nodeGroups.clear(); }

private:
    common::node_group_idx_t initializeLocalNodeGroup(common::ValueVector* nodeIDVector);
    common::node_group_idx_t initializeLocalNodeGroup(common::offset_t nodeOffset);

private:
    common::table_id_t tableID;
    std::vector<std::unique_ptr<common::LogicalType>> dataTypes;
    MemoryManager* mm;
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>> nodeGroups;
};

} // namespace storage
} // namespace kuzu
