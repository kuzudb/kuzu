#pragma once

#include "binder/bound_scan_source.h"
#include "binder/expression/expression.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "index_look_up_info.h"

namespace kuzu {
namespace binder {

struct ExtraBoundCopyFromInfo {
    virtual ~ExtraBoundCopyFromInfo() = default;
    virtual std::unique_ptr<ExtraBoundCopyFromInfo> copy() const = 0;
};

struct BoundCopyFromInfo {
    // Table entry to copy into.
    catalog::TableCatalogEntry* tableEntry;
    // Data source
    std::unique_ptr<BoundBaseScanSource> source;
    // Row offset of input data to generate internal ID.
    std::shared_ptr<Expression> offset;
    expression_vector columnExprs;
    std::vector<bool> defaultColumns;
    std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo;

    BoundCopyFromInfo(catalog::TableCatalogEntry* tableEntry,
        std::unique_ptr<BoundBaseScanSource> source, std::shared_ptr<Expression> offset,
        expression_vector columnExprs, std::vector<bool> defaultColumns,
        std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo)
        : tableEntry{tableEntry}, source{std::move(source)}, offset{offset},
          columnExprs{std::move(columnExprs)}, defaultColumns{std::move(defaultColumns)},
          extraInfo{std::move(extraInfo)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundCopyFromInfo);

private:
    BoundCopyFromInfo(const BoundCopyFromInfo& other)
        : tableEntry{other.tableEntry}, source{other.source->copy()}, offset{other.offset},
          columnExprs{other.columnExprs}, defaultColumns{other.defaultColumns} {
        if (other.extraInfo) {
            extraInfo = other.extraInfo->copy();
        }
    }
};

struct ExtraBoundCopyRelInfo final : public ExtraBoundCopyFromInfo {
    std::vector<IndexLookupInfo> infos;

    ExtraBoundCopyRelInfo() = default;
    ExtraBoundCopyRelInfo(const ExtraBoundCopyRelInfo& other) : infos{copyVector(other.infos)} {}

    inline std::unique_ptr<ExtraBoundCopyFromInfo> copy() const override {
        return std::make_unique<ExtraBoundCopyRelInfo>(*this);
    }
};

struct ExtraBoundCopyRdfInfo final : public ExtraBoundCopyFromInfo {
    BoundCopyFromInfo rInfo;
    BoundCopyFromInfo lInfo;
    BoundCopyFromInfo rrrInfo;
    BoundCopyFromInfo rrlInfo;

    ExtraBoundCopyRdfInfo(BoundCopyFromInfo rInfo, BoundCopyFromInfo lInfo,
        BoundCopyFromInfo rrrInfo, BoundCopyFromInfo rrlInfo)
        : rInfo{std::move(rInfo)}, lInfo{std::move(lInfo)}, rrrInfo{std::move(rrrInfo)},
          rrlInfo{std::move(rrlInfo)} {}
    ExtraBoundCopyRdfInfo(const ExtraBoundCopyRdfInfo& other)
        : rInfo{other.rInfo.copy()}, lInfo{other.lInfo.copy()}, rrrInfo{other.rrrInfo.copy()},
          rrlInfo{other.rrlInfo.copy()} {}

    inline std::unique_ptr<ExtraBoundCopyFromInfo> copy() const override {
        return std::make_unique<ExtraBoundCopyRdfInfo>(*this);
    }
};

class BoundCopyFrom : public BoundStatement {
public:
    explicit BoundCopyFrom(BoundCopyFromInfo info)
        : BoundStatement{common::StatementType::COPY_FROM,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    inline const BoundCopyFromInfo* getInfo() const { return &info; }

private:
    BoundCopyFromInfo info;
};

} // namespace binder
} // namespace kuzu
