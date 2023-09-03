#pragma once

#include "binder/bound_statement.h"
#include "bound_file_scan_info.h"
#include "catalog/table_schema.h"
#include "common/copier_config/copier_config.h"

namespace kuzu {
namespace binder {

// Note: this is a temporary workaround before we can copy RDF in one statement.
// enum class BoundCopyInfoType : uint8_t {
//    NODE = 0,
//    REL = 1,
//    RDF_REL = 2,
//};

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
    common::table_id_t nodeTableID;
    std::shared_ptr<Expression> subjectOffset;
    std::shared_ptr<Expression> predicateOffset;
    std::shared_ptr<Expression> objectOffset;
    std::shared_ptr<Expression> subjectKey;
    std::shared_ptr<Expression> predicateKey;
    std::shared_ptr<Expression> objectKey;

    ExtraBoundCopyRdfRelInfo(common::table_id_t nodeTableID,
        std::shared_ptr<Expression> subjectOffset, std::shared_ptr<Expression> predicateOffset,
        std::shared_ptr<Expression> objectOffset, std::shared_ptr<Expression> subjectKey,
        std::shared_ptr<Expression> predicateKey, std::shared_ptr<Expression> objectKey)
        : nodeTableID{nodeTableID}, subjectOffset{std::move(subjectOffset)},
          predicateOffset{std::move(predicateOffset)}, objectOffset{std::move(objectOffset)},
          subjectKey{std::move(subjectKey)},
          predicateKey{std::move(predicateKey)}, objectKey{std::move(objectKey)} {}
    ExtraBoundCopyRdfRelInfo(const ExtraBoundCopyRdfRelInfo& other)
        : nodeTableID{other.nodeTableID}, subjectOffset{other.subjectOffset},
          predicateOffset{other.predicateOffset}, objectOffset{other.objectOffset},
          subjectKey{other.subjectKey}, predicateKey{other.predicateKey}, objectKey{
                                                                              other.objectKey} {}

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
