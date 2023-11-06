#pragma once

#include <bitset>
#include <map>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace storage {
class Column;
class NodeTable;

class LocalVector {
public:
    LocalVector(const common::LogicalType& dataType, MemoryManager* mm) {
        vector = std::make_unique<common::ValueVector>(dataType, mm);
        vector->setState(std::make_shared<common::DataChunkState>());
        vector->state->selVector->resetSelectorToValuePosBuffer();
    }

    virtual ~LocalVector() = default;

    virtual void scan(common::ValueVector* resultVector) const;
    virtual void lookup(common::sel_t offsetInLocalVector, common::ValueVector* resultVector,
        common::sel_t offsetInResultVector);
    virtual void update(common::sel_t offsetInLocalVector, common::ValueVector* updateVector,
        common::sel_t offsetInUpdateVector);

    std::unique_ptr<common::ValueVector> vector;
    // This mask is mainly to speed the lookup operation up. Otherwise, we have to do binary search
    // to check if the value at an offset has been updated or not.
    std::bitset<common::DEFAULT_VECTOR_CAPACITY> validityMask;
};

class StringLocalVector : public LocalVector {
public:
    explicit StringLocalVector(MemoryManager* mm)
        : LocalVector{common::LogicalType(common::LogicalTypeID::STRING), mm}, ovfStringLength{
                                                                                   0} {};

    void update(common::sel_t offsetInLocalVector, common::ValueVector* updateVector,
        common::sel_t offsetInUpdateVector) final;

    uint64_t ovfStringLength;
};

class StructLocalVector : public LocalVector {
public:
    explicit StructLocalVector(MemoryManager* mm)
        : LocalVector{common::LogicalType{common::LogicalTypeID::STRUCT,
                          std::make_unique<common::StructTypeInfo>()},
              mm} {}

    void scan(common::ValueVector* resultVector) const final;
    void lookup(common::sel_t offsetInLocalVector, common::ValueVector* resultVector,
        common::sel_t offsetInResultVector) final;
    void update(common::sel_t offsetInLocalVector, common::ValueVector* updateVector,
        common::sel_t offsetInUpdateVector) final;
};

struct LocalVectorFactory {
    static std::unique_ptr<LocalVector> createLocalVectorData(
        const common::LogicalType& logicalType, MemoryManager* mm);
};

class LocalColumnChunk {
public:
    explicit LocalColumnChunk(const common::LogicalType& dataType, MemoryManager* mm)
        : dataType{dataType}, mm{mm} {};

    void scan(common::vector_idx_t vectorIdx, common::ValueVector* resultVector);
    void lookup(common::vector_idx_t vectorIdx, common::sel_t offsetInLocalVector,
        common::ValueVector* resultVector, common::sel_t offsetInResultVector);
    void update(common::vector_idx_t vectorIdx, common::sel_t offsetInVector,
        common::ValueVector* vectorToWriteFrom, common::sel_t pos);

    std::map<common::vector_idx_t, std::unique_ptr<LocalVector>> vectors;
    common::LogicalType dataType;
    MemoryManager* mm;
};

class LocalColumn {
public:
    explicit LocalColumn(Column* column, bool enableCompression)
        : column{column}, enableCompression{enableCompression} {};
    virtual ~LocalColumn() = default;

    virtual void scan(common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookup(common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void update(
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector, MemoryManager* mm);
    virtual void update(common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector, MemoryManager* mm);

    virtual void prepareCommit();

    virtual void prepareCommitForChunk(common::node_group_idx_t nodeGroupIdx);
    void commitLocalChunkOutOfPlace(
        common::node_group_idx_t nodeGroupIdx, LocalColumnChunk* localChunk);
    void commitLocalChunkInPlace(
        common::node_group_idx_t nodeGroupIdx, LocalColumnChunk* localChunk);

protected:
    std::map<common::node_group_idx_t, std::unique_ptr<LocalColumnChunk>> chunks;
    Column* column;
    bool enableCompression;
};

class StringLocalColumn : public LocalColumn {
public:
    explicit StringLocalColumn(Column* column, bool enableCompression)
        : LocalColumn{column, enableCompression} {};

    void prepareCommitForChunk(common::node_group_idx_t nodeGroupIdx) final;
};

class VarListLocalColumn : public LocalColumn {
public:
    explicit VarListLocalColumn(Column* column, bool enableCompression)
        : LocalColumn{column, enableCompression} {};

    void prepareCommitForChunk(common::node_group_idx_t nodeGroupIdx) final;
};

class StructLocalColumn : public LocalColumn {
public:
    explicit StructLocalColumn(Column* column, bool enableCompression);

    void scan(common::ValueVector* nodeIDVector, common::ValueVector* resultVector) final;
    void lookup(common::ValueVector* nodeIDVector, common::ValueVector* resultVector) final;
    void update(common::ValueVector* nodeIDVector, common::ValueVector* propertyVector,
        MemoryManager* mm) final;
    void update(common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector, MemoryManager* mm) final;

    void prepareCommitForChunk(common::node_group_idx_t nodeGroupIdx) final;

private:
    std::vector<std::unique_ptr<LocalColumn>> fields;
};

struct LocalColumnFactory {
    static std::unique_ptr<LocalColumn> createLocalColumn(Column* column, bool enableCompression);
};

class LocalTable {
public:
    explicit LocalTable(NodeTable* table, bool enableCompression)
        : table{table}, enableCompression{enableCompression} {};

    void scan(common::ValueVector* nodeIDVector, const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void update(common::column_id_t columnID, common::ValueVector* nodeIDVector,
        common::ValueVector* propertyVector, MemoryManager* mm);
    void update(common::column_id_t columnID, common::offset_t nodeOffset,
        common::ValueVector* propertyVector, common::sel_t posInPropertyVector, MemoryManager* mm);

    void prepareCommit();

private:
    std::map<common::column_id_t, std::unique_ptr<LocalColumn>> columns;
    NodeTable* table;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
