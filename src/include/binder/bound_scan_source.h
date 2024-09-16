#pragma once

#include "binder/bound_statement.h"
#include "binder/copy/bound_table_scan_info.h"
#include "common/enums/scan_source_type.h"

namespace kuzu {
namespace binder {

struct BoundBaseScanSource {
    common::ScanSourceType type;

    explicit BoundBaseScanSource(common::ScanSourceType type) : type{type} {}
    virtual ~BoundBaseScanSource() = default;

    virtual expression_vector getColumns() = 0;
    virtual expression_vector getWarningColumns() { return expression_vector{}; };
    virtual bool getIgnoreErrorsOption() { return common::CopyConstants::DEFAULT_IGNORE_ERRORS; };
    virtual common::column_id_t getNumWarningDataColumns() { return 0; }

    virtual std::unique_ptr<BoundBaseScanSource> copy() const = 0;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const BoundBaseScanSource&, const TARGET&>(*this);
    }

protected:
    BoundBaseScanSource(const BoundBaseScanSource& other) : type{other.type} {}
};

struct BoundEmptyScanSource : public BoundBaseScanSource {
    BoundEmptyScanSource() : BoundBaseScanSource{common::ScanSourceType::EMPTY} {}
    BoundEmptyScanSource(const BoundEmptyScanSource& other) : BoundBaseScanSource{other} {}

    expression_vector getColumns() override { return expression_vector{}; }

    std::unique_ptr<BoundBaseScanSource> copy() const override {
        return std::make_unique<BoundEmptyScanSource>(*this);
    }
};

struct BoundTableScanSource : public BoundBaseScanSource {
    BoundTableScanSourceInfo info;

    explicit BoundTableScanSource(common::ScanSourceType type, BoundTableScanSourceInfo info)
        : BoundBaseScanSource{type}, info{std::move(info)} {}
    BoundTableScanSource(const BoundTableScanSource& other)
        : BoundBaseScanSource{other}, info{other.info.copy()} {}

    expression_vector getColumns() override { return info.columns; }
    expression_vector getWarningColumns() override {
        expression_vector warningDataExprs;
        for (common::column_id_t i = info.bindData->numWarningDataColumns; i >= 1; --i) {
            KU_ASSERT(i < info.columns.size());
            warningDataExprs.push_back(info.columns[info.columns.size() - i]);
        }
        return warningDataExprs;
    }
    bool getIgnoreErrorsOption() override {
        bool ignoreErrors = common::CopyConstants::DEFAULT_IGNORE_ERRORS;
        if (type == common::ScanSourceType::FILE) {
            auto bindData = info.bindData->constPtrCast<function::ScanBindData>();
            ignoreErrors = bindData->config.getOption(
                common::CopyConstants::IGNORE_ERRORS_OPTION_NAME, ignoreErrors);
        }
        return ignoreErrors;
    }
    common::column_id_t getNumWarningDataColumns() override {
        return info.bindData->numWarningDataColumns;
    }

    std::unique_ptr<BoundBaseScanSource> copy() const override {
        return std::make_unique<BoundTableScanSource>(*this);
    }
};

struct BoundQueryScanSource : public BoundBaseScanSource {
    // Use shared ptr to avoid copy BoundStatement.
    // We should consider implement a copy constructor though.
    std::shared_ptr<BoundStatement> statement;

    explicit BoundQueryScanSource(std::shared_ptr<BoundStatement> statement)
        : BoundBaseScanSource{common::ScanSourceType::QUERY}, statement{std::move(statement)} {}
    BoundQueryScanSource(const BoundQueryScanSource& other)
        : BoundBaseScanSource{other}, statement{other.statement} {}

    expression_vector getColumns() override {
        return statement->getStatementResult()->getColumns();
    }

    std::unique_ptr<BoundBaseScanSource> copy() const override {
        return std::make_unique<BoundQueryScanSource>(*this);
    }
};

} // namespace binder
} // namespace kuzu
