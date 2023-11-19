#pragma once

#include "common/enums/delete_type.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

class NodeDeleteExecutor {
public:
    NodeDeleteExecutor(common::DeleteNodeType deleteType, const DataPos& nodeIDPos)
        : deleteType{deleteType}, nodeIDPos{nodeIDPos}, nodeIDVector(nullptr) {}
    virtual ~NodeDeleteExecutor() = default;

    virtual void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void delete_(ExecutionContext* context) = 0;

    virtual std::unique_ptr<NodeDeleteExecutor> copy() const = 0;

protected:
    common::DeleteNodeType deleteType;
    DataPos nodeIDPos;
    common::ValueVector* nodeIDVector;
    std::unique_ptr<storage::RelDetachDeleteState> detachDeleteState;
};

class SingleLabelNodeDeleteExecutor final : public NodeDeleteExecutor {
public:
    SingleLabelNodeDeleteExecutor(storage::NodeTable* table,
        std::unordered_set<storage::RelTable*> fwdRelTables,
        std::unordered_set<storage::RelTable*> bwdRelTables, common::DeleteNodeType deleteType,
        const DataPos& nodeIDPos)
        : NodeDeleteExecutor(deleteType, nodeIDPos), table{table}, fwdRelTables{fwdRelTables},
          bwdRelTables{bwdRelTables} {}
    SingleLabelNodeDeleteExecutor(const SingleLabelNodeDeleteExecutor& other)
        : NodeDeleteExecutor(other.deleteType, other.nodeIDPos), table{other.table},
          fwdRelTables{other.fwdRelTables}, bwdRelTables{other.bwdRelTables} {}

    void init(ResultSet* resultSet, ExecutionContext* context) override;
    void delete_(ExecutionContext* context) override;

    inline std::unique_ptr<NodeDeleteExecutor> copy() const override {
        return std::make_unique<SingleLabelNodeDeleteExecutor>(*this);
    }

private:
    storage::NodeTable* table;
    std::unique_ptr<common::ValueVector> pkVector;
    std::unordered_set<storage::RelTable*> fwdRelTables;
    std::unordered_set<storage::RelTable*> bwdRelTables;
};

class MultiLabelNodeDeleteExecutor final : public NodeDeleteExecutor {
    using rel_tables_set_t = std::unordered_set<storage::RelTable*>;

public:
    MultiLabelNodeDeleteExecutor(
        std::unordered_map<common::table_id_t, storage::NodeTable*> tableIDToTableMap,
        std::unordered_map<common::table_id_t, rel_tables_set_t> tableIDToFwdRelTablesMap,
        std::unordered_map<common::table_id_t, rel_tables_set_t> tableIDToBwdRelTablesMap,
        common::DeleteNodeType deleteType, const DataPos& nodeIDPos)
        : NodeDeleteExecutor(deleteType, nodeIDPos),
          tableIDToTableMap{std::move(tableIDToTableMap)}, tableIDToFwdRelTablesMap{std::move(
                                                               tableIDToFwdRelTablesMap)},
          tableIDToBwdRelTablesMap{std::move(tableIDToBwdRelTablesMap)} {}
    MultiLabelNodeDeleteExecutor(const MultiLabelNodeDeleteExecutor& other)
        : NodeDeleteExecutor(other.deleteType, other.nodeIDPos),
          tableIDToTableMap{other.tableIDToTableMap},
          tableIDToFwdRelTablesMap{other.tableIDToFwdRelTablesMap},
          tableIDToBwdRelTablesMap{other.tableIDToBwdRelTablesMap} {}

    void init(ResultSet* resultSet, ExecutionContext* context) override;
    void delete_(ExecutionContext* context) override;

    inline std::unique_ptr<NodeDeleteExecutor> copy() const override {
        return std::make_unique<MultiLabelNodeDeleteExecutor>(*this);
    }

private:
    std::unordered_map<common::table_id_t, storage::NodeTable*> tableIDToTableMap;
    std::unordered_map<common::table_id_t, rel_tables_set_t> tableIDToFwdRelTablesMap;
    std::unordered_map<common::table_id_t, rel_tables_set_t> tableIDToBwdRelTablesMap;
    std::unordered_map<common::table_id_t, std::unique_ptr<common::ValueVector>> pkVectors;
};

class RelDeleteExecutor {
public:
    RelDeleteExecutor(
        const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos, const DataPos& relIDPos)
        : srcNodeIDPos{srcNodeIDPos}, dstNodeIDPos{dstNodeIDPos}, relIDPos{relIDPos},
          srcNodeIDVector(nullptr), dstNodeIDVector(nullptr), relIDVector(nullptr) {}
    virtual ~RelDeleteExecutor() = default;

    void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void delete_(ExecutionContext* context) = 0;

    virtual std::unique_ptr<RelDeleteExecutor> copy() const = 0;

protected:
    DataPos srcNodeIDPos;
    DataPos dstNodeIDPos;
    DataPos relIDPos;

    common::ValueVector* srcNodeIDVector;
    common::ValueVector* dstNodeIDVector;
    common::ValueVector* relIDVector;
};

class SingleLabelRelDeleteExecutor final : public RelDeleteExecutor {
public:
    SingleLabelRelDeleteExecutor(storage::RelTable* table, const DataPos& srcNodeIDPos,
        const DataPos& dstNodeIDPos, const DataPos& relIDPos)
        : RelDeleteExecutor(srcNodeIDPos, dstNodeIDPos, relIDPos), table{table} {}
    SingleLabelRelDeleteExecutor(const SingleLabelRelDeleteExecutor& other) = default;

    void delete_(ExecutionContext* context) override;

    inline std::unique_ptr<RelDeleteExecutor> copy() const override {
        return std::make_unique<SingleLabelRelDeleteExecutor>(*this);
    }

private:
    storage::RelTable* table;
};

class MultiLabelRelDeleteExecutor final : public RelDeleteExecutor {

public:
    MultiLabelRelDeleteExecutor(
        std::unordered_map<common::table_id_t, storage::RelTable*> tableIDToTableMap,
        const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos, const DataPos& relIDPos)
        : RelDeleteExecutor(srcNodeIDPos, dstNodeIDPos, relIDPos), tableIDToTableMap{std::move(
                                                                       tableIDToTableMap)} {}
    MultiLabelRelDeleteExecutor(const MultiLabelRelDeleteExecutor& other) = default;

    void delete_(ExecutionContext* context) override;

    inline std::unique_ptr<RelDeleteExecutor> copy() const override {
        return std::make_unique<MultiLabelRelDeleteExecutor>(*this);
    }

private:
    std::unordered_map<common::table_id_t, storage::RelTable*> tableIDToTableMap;
};

} // namespace processor
} // namespace kuzu
