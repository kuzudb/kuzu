#pragma once

#include <map>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace storage {

using offset_to_row_idx_t = std::map<common::offset_t, common::row_idx_t>;

class TableData;

// TODO(Guodong): Instead of using ValueVector, we should switch to ColumnChunk.
// This class is used to store a chunk of local changes to a column in a node group.
// Values are stored inside `vector`.
class LocalVector {
public:
    LocalVector(const common::LogicalType& dataType, MemoryManager* mm) : numValues{0} {
        vector = std::make_unique<common::ValueVector>(dataType, mm);
        vector->setState(std::make_shared<common::DataChunkState>());
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
    LocalVectorCollection(std::unique_ptr<common::LogicalType> dataType, MemoryManager* mm)
        : dataType{std::move(dataType)}, mm{mm}, numRows{0} {}

    void read(common::row_idx_t rowIdx, common::ValueVector* outputVector,
        common::sel_t posInOutputVector);
    common::row_idx_t append(common::ValueVector* vector);

    inline uint64_t getNumRows() { return numRows; }
    inline LocalVector* getLocalVector(common::row_idx_t rowIdx) {
        auto vectorIdx = rowIdx >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
        KU_ASSERT(vectorIdx < vectors.size());
        return vectors[vectorIdx].get();
    }

    common::row_idx_t getRowIdx(common::offset_t nodeOffset);

private:
    void prepareAppend();

private:
    std::unique_ptr<common::LogicalType> dataType;
    MemoryManager* mm;
    std::vector<std::unique_ptr<LocalVector>> vectors;
    common::row_idx_t numRows;
};

class LocalNodeGroup {
    friend class NodeTableData;

public:
    LocalNodeGroup(
        const std::vector<std::unique_ptr<common::LogicalType>>& dataTypes, MemoryManager* mm) {
        columns.resize(dataTypes.size());
        for (auto i = 0u; i < dataTypes.size(); ++i) {
            columns[i] = std::make_unique<LocalVectorCollection>(dataTypes[i]->copy(), mm);
        }
    }

    inline LocalVectorCollection* getLocalColumnChunk(common::column_id_t columnID) {
        return columns[columnID].get();
    }

protected:
    std::vector<std::unique_ptr<LocalVectorCollection>> columns;
};

class LocalTable {
public:
    explicit LocalTable(common::table_id_t tableID,
        std::vector<std::unique_ptr<common::LogicalType>> dataTypes, MemoryManager* mm)
        : tableID{tableID}, dataTypes{std::move(dataTypes)}, mm{mm} {};
    virtual ~LocalTable() = default;

    virtual void scan(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors) = 0;
    virtual void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors) = 0;
    virtual void clear() = 0;

protected:
    common::table_id_t tableID;
    std::vector<std::unique_ptr<common::LogicalType>> dataTypes;
    MemoryManager* mm;
};

} // namespace storage
} // namespace kuzu
