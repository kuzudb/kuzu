#pragma once

#include "binder/bound_statement.h"
#include "bound_file_scan_info.h"
#include "catalog/table_schema.h"

namespace kuzu {
namespace binder {

struct ExtraBoundCopyFromInfo {
    virtual ~ExtraBoundCopyFromInfo() = default;
    virtual std::unique_ptr<ExtraBoundCopyFromInfo> copy() = 0;
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
        : tableSchema{other.tableSchema}, fileScanInfo{other.fileScanInfo->copy()},
          containsSerial{other.containsSerial} {
        if (other.extraInfo) {
            extraInfo = other.extraInfo->copy();
        }
    }

    inline std::unique_ptr<BoundCopyFromInfo> copy() {
        return std::make_unique<BoundCopyFromInfo>(*this);
    }
};

struct ExtraBoundCopyRelInfo : public ExtraBoundCopyFromInfo {
    catalog::TableSchema* srcTableSchema;
    catalog::TableSchema* dstTableSchema;
    std::shared_ptr<Expression> srcOffset;
    std::shared_ptr<Expression> dstOffset;
    std::shared_ptr<Expression> srcKey;
    std::shared_ptr<Expression> dstKey;

    ExtraBoundCopyRelInfo(catalog::TableSchema* srcTableSchema,
        catalog::TableSchema* dstTableSchema, std::shared_ptr<Expression> srcOffset,
        std::shared_ptr<Expression> dstOffset, std::shared_ptr<Expression> srcKey,
        std::shared_ptr<Expression> dstKey)
        : srcTableSchema{srcTableSchema}, dstTableSchema{dstTableSchema}, srcOffset{std::move(
                                                                              srcOffset)},
          dstOffset{std::move(dstOffset)}, srcKey{std::move(srcKey)}, dstKey{std::move(dstKey)} {}
    ExtraBoundCopyRelInfo(const ExtraBoundCopyRelInfo& other)
        : srcTableSchema{other.srcTableSchema},
          dstTableSchema{other.dstTableSchema}, srcOffset{other.srcOffset},
          dstOffset{other.dstOffset}, srcKey{other.srcKey}, dstKey{other.dstKey} {}

    inline std::unique_ptr<ExtraBoundCopyFromInfo> copy() final {
        return std::make_unique<ExtraBoundCopyRelInfo>(*this);
    }
};

struct ExtraBoundCopyRdfRelInfo : public ExtraBoundCopyFromInfo {
    std::shared_ptr<Expression> subjectOffset;
    std::shared_ptr<Expression> objectOffset;

    ExtraBoundCopyRdfRelInfo(
        std::shared_ptr<Expression> subjectOffset, std::shared_ptr<Expression> objectOffset)
        : subjectOffset{std::move(subjectOffset)}, objectOffset{std::move(objectOffset)} {}
    ExtraBoundCopyRdfRelInfo(const ExtraBoundCopyRdfRelInfo& other)
        : subjectOffset{other.subjectOffset}, objectOffset{other.objectOffset} {}

    inline std::unique_ptr<ExtraBoundCopyFromInfo> copy() final {
        return std::make_unique<ExtraBoundCopyRdfRelInfo>(*this);
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
