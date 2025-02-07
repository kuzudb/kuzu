#pragma once

#include "binder/bound_scan_source.h"
#include "binder/expression/expression.h"
#include "common/enums/column_evaluate_type.h"
#include "index_look_up_info.h"

namespace kuzu::catalog {
class TableCatalogEntry;
} // namespace kuzu::catalog

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
    // Data source.
    std::unique_ptr<BoundBaseScanSource> source;
    // Row offset.
    std::shared_ptr<Expression> offset;
    expression_vector columnExprs;
    std::vector<common::ColumnEvaluateType> columnEvaluateTypes;
    std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo;

    BoundCopyFromInfo(catalog::TableCatalogEntry* tableEntry,
        std::unique_ptr<BoundBaseScanSource> source, std::shared_ptr<Expression> offset,
        expression_vector columnExprs, std::vector<common::ColumnEvaluateType> columnEvaluateTypes,
        std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo)
        : tableEntry{tableEntry}, source{std::move(source)}, offset{std::move(offset)},
          columnExprs{std::move(columnExprs)}, columnEvaluateTypes{std::move(columnEvaluateTypes)},
          extraInfo{std::move(extraInfo)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCopyFromInfo);

    expression_vector getSourceColumns() const {
        return source ? source->getColumns() : expression_vector{};
    }
    common::column_id_t getNumWarningColumns() const {
        return source ? source->getNumWarningDataColumns() : 0;
    }
    bool getIgnoreErrorsOption() const { return source ? source->getIgnoreErrorsOption() : false; }

private:
    BoundCopyFromInfo(const BoundCopyFromInfo& other)
        : tableEntry{other.tableEntry}, offset{other.offset}, columnExprs{other.columnExprs},
          columnEvaluateTypes{other.columnEvaluateTypes} {
        source = other.source ? other.source->copy() : nullptr;
        if (other.extraInfo) {
            extraInfo = other.extraInfo->copy();
        }
    }
};

struct ExtraBoundCopyRelInfo final : ExtraBoundCopyFromInfo {
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

class BoundCopyFrom final : public BoundStatement {
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
