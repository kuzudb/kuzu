#pragma once

#include <map>

#include "common/column_data_format.h"
#include "common/enums/table_type.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace storage {
class TableData;

using offset_to_row_idx_t = std::map<common::offset_t, common::row_idx_t>;
using offset_set_t = std::unordered_set<common::offset_t>;
using offset_to_offset_to_row_idx_t = std::map<common::offset_t, offset_to_row_idx_t>;
using offset_to_offset_set_t = std::map<common::offset_t, std::unordered_set<common::offset_t>>;

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
    inline uint64_t getNumRows() { return numRows; }
    inline LocalVector* getLocalVector(common::row_idx_t rowIdx) {
        auto vectorIdx = rowIdx >> common::DEFAULT_VECTOR_CAPACITY_LOG_2;
        KU_ASSERT(vectorIdx < vectors.size());
        return vectors[vectorIdx].get();
    }

    std::unique_ptr<LocalVectorCollection> getStructChildVectorCollection(
        common::struct_field_idx_t idx);

    common::row_idx_t append(common::ValueVector* vector);

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
    LocalNodeGroup(common::offset_t nodeGroupStartOffset,
        std::vector<common::LogicalType*> dataTypes, MemoryManager* mm);
    virtual ~LocalNodeGroup() = default;

    inline LocalVectorCollection* getLocalColumnChunk(common::column_id_t columnID) {
        return chunks[columnID].get();
    }

protected:
    common::offset_t nodeGroupStartOffset;
    std::vector<std::unique_ptr<LocalVectorCollection>> chunks;
};

class LocalTableData {
    friend class NodeTableData;

public:
    LocalTableData(std::vector<common::LogicalType*> dataTypes, MemoryManager* mm,
        common::ColumnDataFormat dataFormat)
        : dataTypes{std::move(dataTypes)}, mm{mm}, dataFormat{dataFormat} {}
    virtual ~LocalTableData() = default;

    inline void clear() { nodeGroups.clear(); }

protected:
    virtual LocalNodeGroup* getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) = 0;

protected:
    std::vector<common::LogicalType*> dataTypes;
    MemoryManager* mm;
    common::ColumnDataFormat dataFormat;
    std::unordered_map<common::node_group_idx_t, std::unique_ptr<LocalNodeGroup>> nodeGroups;
};

class Column;
class LocalTable {
public:
    LocalTable(common::table_id_t tableID, common::TableType tableType)
        : tableID{tableID}, tableType{tableType} {};

    LocalTableData* getOrCreateLocalTableData(const std::vector<std::unique_ptr<Column>>& columns,
        MemoryManager* mm, common::ColumnDataFormat dataFormat = common::ColumnDataFormat::REGULAR,
        common::vector_idx_t dataIdx = 0);
    inline LocalTableData* getLocalTableData(common::vector_idx_t dataIdx) {
        KU_ASSERT(dataIdx < localTableDataCollection.size());
        return localTableDataCollection[dataIdx].get();
    }

private:
    common::table_id_t tableID;
    common::TableType tableType;
    // For a node table, it should only contain one LocalTableData, while a rel table should contain
    // two, one for each direction.
    std::vector<std::unique_ptr<LocalTableData>> localTableDataCollection;
};

} // namespace storage
} // namespace kuzu
