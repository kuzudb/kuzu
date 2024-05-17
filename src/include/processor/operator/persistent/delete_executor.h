#pragma once

#include <utility>

#include "common/enums/delete_type.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

struct NodeDeleteInfo {
    common::DeleteNodeType deleteType;
    DataPos nodeIDPos;

    NodeDeleteInfo(common::DeleteNodeType deleteType, const DataPos& nodeIDPos)
        : deleteType{deleteType}, nodeIDPos{nodeIDPos} {};
    EXPLICIT_COPY_DEFAULT_MOVE(NodeDeleteInfo);

private:
    NodeDeleteInfo(const NodeDeleteInfo& other)
        : deleteType{other.deleteType}, nodeIDPos{other.nodeIDPos} {}
};

class NodeDeleteExecutor {
public:
    explicit NodeDeleteExecutor(NodeDeleteInfo info)
        : info{std::move(info)}, nodeIDVector(nullptr) {}
    NodeDeleteExecutor(const NodeDeleteExecutor& other) : info{other.info.copy()} {}
    virtual ~NodeDeleteExecutor() = default;

    virtual void init(ResultSet* resultSet, ExecutionContext* context);

    virtual void delete_(ExecutionContext* context) = 0;

    virtual std::unique_ptr<NodeDeleteExecutor> copy() const = 0;

protected:
    NodeDeleteInfo info;

    common::ValueVector* nodeIDVector;
    std::unique_ptr<storage::RelDetachDeleteState> detachDeleteState;
};

struct ExtraNodeDeleteInfo {
    storage::NodeTable* table;
    std::unordered_set<storage::RelTable*> fwdRelTables;
    std::unordered_set<storage::RelTable*> bwdRelTables;
    DataPos pkPos;
    common::ValueVector* pkVector;

    ExtraNodeDeleteInfo(storage::NodeTable* table,
        std::unordered_set<storage::RelTable*> fwdRelTables,
        std::unordered_set<storage::RelTable*> bwdRelTables, DataPos pkPos)
        : table{table}, fwdRelTables{std::move(fwdRelTables)},
          bwdRelTables{std::move(bwdRelTables)}, pkPos{std::move(pkPos)} {};
    EXPLICIT_COPY_DEFAULT_MOVE(ExtraNodeDeleteInfo);

private:
    ExtraNodeDeleteInfo(const ExtraNodeDeleteInfo& other)
        : table{other.table}, fwdRelTables{other.fwdRelTables}, bwdRelTables{other.bwdRelTables},
          pkPos{other.pkPos} {}
};

class SingleLabelNodeDeleteExecutor final : public NodeDeleteExecutor {
public:
    SingleLabelNodeDeleteExecutor(NodeDeleteInfo info, ExtraNodeDeleteInfo extraInfo)
        : NodeDeleteExecutor(std::move(info)), extraInfo{std::move(extraInfo)} {}
    SingleLabelNodeDeleteExecutor(const SingleLabelNodeDeleteExecutor& other)
        : NodeDeleteExecutor(other), extraInfo{other.extraInfo.copy()} {}

    void init(ResultSet* resultSet, ExecutionContext* context) override;
    void delete_(ExecutionContext* context) override;

    std::unique_ptr<NodeDeleteExecutor> copy() const override {
        return std::make_unique<SingleLabelNodeDeleteExecutor>(*this);
    }

private:
    ExtraNodeDeleteInfo extraInfo;
};

class MultiLabelNodeDeleteExecutor final : public NodeDeleteExecutor {
public:
    MultiLabelNodeDeleteExecutor(NodeDeleteInfo info,
        common::table_id_map_t<ExtraNodeDeleteInfo> extraInfos)
        : NodeDeleteExecutor(std::move(info)), extraInfos{std::move(extraInfos)} {}
    MultiLabelNodeDeleteExecutor(const MultiLabelNodeDeleteExecutor& other)
        : NodeDeleteExecutor(other), extraInfos{copyMap(other.extraInfos)} {}

    void init(ResultSet* resultSet, ExecutionContext* context) override;
    void delete_(ExecutionContext* context) override;

    std::unique_ptr<NodeDeleteExecutor> copy() const override {
        return std::make_unique<MultiLabelNodeDeleteExecutor>(*this);
    }

private:
    common::table_id_map_t<ExtraNodeDeleteInfo> extraInfos;
};

class RelDeleteExecutor {
public:
    RelDeleteExecutor(const DataPos& srcNodeIDPos, const DataPos& dstNodeIDPos,
        const DataPos& relIDPos)
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
        : RelDeleteExecutor(srcNodeIDPos, dstNodeIDPos, relIDPos),
          tableIDToTableMap{std::move(tableIDToTableMap)} {}
    MultiLabelRelDeleteExecutor(const MultiLabelRelDeleteExecutor& other) = default;

    void delete_(ExecutionContext* context) override;

    inline std::unique_ptr<RelDeleteExecutor> copy() const override {
        return std::make_unique<MultiLabelRelDeleteExecutor>(*this);
    }

private:
    common::table_id_map_t<storage::RelTable*> tableIDToTableMap;
};

} // namespace processor
} // namespace kuzu
