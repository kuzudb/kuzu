#pragma once

#include "binder/bound_statement.h"
#include "bound_file_scan_info.h"
#include "catalog/table_schema.h"
#include "index_look_up_info.h"

namespace kuzu {
namespace binder {

struct ExtraBoundCopyFromInfo {
    virtual ~ExtraBoundCopyFromInfo() = default;
    virtual std::unique_ptr<ExtraBoundCopyFromInfo> copy() const = 0;
};

struct BoundCopyFromInfo {
    catalog::TableSchema* tableSchema;
    std::unique_ptr<BoundFileScanInfo> fileScanInfo;
    bool containsSerial;
    std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo;

    BoundCopyFromInfo(catalog::TableSchema* tableSchema,
        std::unique_ptr<BoundFileScanInfo> fileScanInfo, bool containsSerial,
        std::unique_ptr<ExtraBoundCopyFromInfo> extraInfo)
        : tableSchema{tableSchema}, fileScanInfo{std::move(fileScanInfo)},
          containsSerial{containsSerial}, extraInfo{std::move(extraInfo)} {}
    BoundCopyFromInfo(const BoundCopyFromInfo& other)
        : tableSchema{other.tableSchema}, containsSerial{other.containsSerial} {
        if (other.fileScanInfo) {
            fileScanInfo = other.fileScanInfo->copy();
        }
        if (other.extraInfo) {
            extraInfo = other.extraInfo->copy();
        }
    }

    inline std::unique_ptr<BoundCopyFromInfo> copy() const {
        return std::make_unique<BoundCopyFromInfo>(*this);
    }
};

struct ExtraBoundCopyRelInfo final : public ExtraBoundCopyFromInfo {
    std::vector<std::unique_ptr<IndexLookupInfo>> infos;
    std::shared_ptr<Expression> fromOffset;
    std::shared_ptr<Expression> toOffset;
    expression_vector propertyColumns;

    ExtraBoundCopyRelInfo() = default;
    ExtraBoundCopyRelInfo(const ExtraBoundCopyRelInfo& other)
        : infos{IndexLookupInfo::copy(other.infos)}, fromOffset{other.fromOffset},
          toOffset{other.toOffset}, propertyColumns{other.propertyColumns} {}

    inline std::unique_ptr<ExtraBoundCopyFromInfo> copy() const override {
        return std::make_unique<ExtraBoundCopyRelInfo>(*this);
    }
};

struct ExtraBoundCopyRdfInfo final : public ExtraBoundCopyFromInfo {
    std::unique_ptr<BoundCopyFromInfo> rInfo;
    std::unique_ptr<BoundCopyFromInfo> lInfo;
    std::unique_ptr<BoundCopyFromInfo> rrrInfo;
    std::unique_ptr<BoundCopyFromInfo> rrlInfo;

    ExtraBoundCopyRdfInfo() = default;
    ExtraBoundCopyRdfInfo(const ExtraBoundCopyRdfInfo& other)
        : rInfo{other.rInfo->copy()}, lInfo{other.lInfo->copy()}, rrrInfo{other.rrrInfo->copy()},
          rrlInfo{other.rrlInfo->copy()} {}

    inline std::unique_ptr<ExtraBoundCopyFromInfo> copy() const override {
        return std::make_unique<ExtraBoundCopyRdfInfo>(*this);
    }
};

class BoundCopyFrom : public BoundStatement {
public:
    explicit BoundCopyFrom(std::unique_ptr<BoundCopyFromInfo> info)
        : BoundStatement{common::StatementType::COPY_FROM,
              BoundStatementResult::createSingleStringColumnResult()},
          info{std::move(info)} {}

    inline BoundCopyFromInfo* getInfo() const { return info.get(); }

private:
    std::unique_ptr<BoundCopyFromInfo> info;
};

} // namespace binder
} // namespace kuzu
