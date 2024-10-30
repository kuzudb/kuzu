#pragma once

#include "binder/bound_scan_source.h"
#include "binder/expression/expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/enums/column_evaluate_type.h"
#include "index_look_up_info.h"

namespace kuzu {
namespace binder {

struct ExtraBoundCopyFromInfo {
    virtual ~ExtraBoundCopyFromInfo() = default;
    virtual std::unique_ptr<ExtraBoundCopyFromInfo> copy() const = 0;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }
};

struct BoundCopyFromInfo {
    // Table entry to copy into.
    catalog::TableCatalogEntry* tableEntry;
    // Data source
    std::unique_ptr<BoundBaseScanSource> source;
    // Row offset of input data to generate internal ID.
    std::shared_ptr<Expression> offset;
    expression_vector columnExprs;
    std::vector<common::ColumnEvaluateType> columnEvaluateTypes;
    std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo;

    BoundCopyFromInfo(catalog::TableCatalogEntry* tableEntry,
        std::unique_ptr<BoundBaseScanSource> source, std::shared_ptr<Expression> offset,
        expression_vector columnExprs, std::vector<common::ColumnEvaluateType> columnEvaluateTypes,
        std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo)
        : tableEntry{tableEntry}, source{std::move(source)}, offset{offset},
          columnExprs{std::move(columnExprs)}, columnEvaluateTypes{std::move(columnEvaluateTypes)},
          extraInfo{std::move(extraInfo)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCopyFromInfo);

private:
    BoundCopyFromInfo(const BoundCopyFromInfo& other)
        : tableEntry{other.tableEntry}, source{other.source->copy()}, offset{other.offset},
          columnExprs{other.columnExprs}, columnEvaluateTypes{other.columnEvaluateTypes} {
        if (other.extraInfo) {
            extraInfo = other.extraInfo->copy();
        }
    }
};

struct ExtraBoundCopyRelInfo final : public ExtraBoundCopyFromInfo {
    // We process internal ID column as offset (INT64) column until partitioner. In partitioner,
    // we need to manually change offset(INT64) type to internal ID type.
    std::vector<common::idx_t> internalIDColumnIndices;
    std::vector<IndexLookupInfo> infos;

    ExtraBoundCopyRelInfo(std::vector<common::idx_t> internalIDColumnIndices,
        std::vector<IndexLookupInfo> infos)
        : internalIDColumnIndices{std::move(internalIDColumnIndices)}, infos{std::move(infos)} {}
    ExtraBoundCopyRelInfo(const ExtraBoundCopyRelInfo& other) = default;

    std::unique_ptr<ExtraBoundCopyFromInfo> copy() const override {
        return std::make_unique<ExtraBoundCopyRelInfo>(*this);
    }
};

class BoundCopyFrom : public BoundStatement {
    static constexpr common::StatementType statementType_ = common::StatementType::COPY_FROM;

public:
    explicit BoundCopyFrom(BoundCopyFromInfo info)
        : BoundStatement{statementType_, BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    const BoundCopyFromInfo* getInfo() const { return &info; }

private:
    BoundCopyFromInfo info;
};

} // namespace binder
} // namespace kuzu
