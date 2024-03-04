#pragma once

#include "binder/bound_statement.h"
#include "binder/copy/bound_file_scan_info.h"
#include "common/enums/scan_source_type.h"

namespace kuzu {
namespace binder {

struct BoundBaseScanSource {
    common::ScanSourceType type;

    explicit BoundBaseScanSource(common::ScanSourceType type) : type{type} {}
    virtual ~BoundBaseScanSource() = default;

    virtual expression_vector getColumns() = 0;

    virtual std::unique_ptr<BoundBaseScanSource> copy() const = 0;

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

struct BoundFileScanSource : public BoundBaseScanSource {
    BoundFileScanInfo fileScanInfo;

    explicit BoundFileScanSource(BoundFileScanInfo fileScanInfo)
        : BoundBaseScanSource{common::ScanSourceType::FILE}, fileScanInfo{std::move(fileScanInfo)} {
    }
    BoundFileScanSource(const BoundFileScanSource& other)
        : BoundBaseScanSource{other}, fileScanInfo{other.fileScanInfo.copy()} {}

    expression_vector getColumns() override { return fileScanInfo.columns; }

    std::unique_ptr<BoundBaseScanSource> copy() const override {
        return std::make_unique<BoundFileScanSource>(*this);
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
