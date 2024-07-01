#pragma once

#include "binder/expression/expression.h"
#include "common/enums/alter_type.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace binder {

struct BoundExtraAlterInfo {
    virtual ~BoundExtraAlterInfo() = default;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const BoundExtraAlterInfo*, const TARGET*>(this);
    }
    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const BoundExtraAlterInfo&, const TARGET&>(*this);
    }

    virtual std::unique_ptr<BoundExtraAlterInfo> copy() const = 0;
};

struct BoundAlterInfo {
    common::AlterType alterType;
    std::string tableName;
    common::table_id_t tableID;
    std::unique_ptr<BoundExtraAlterInfo> extraInfo;

    BoundAlterInfo(common::AlterType alterType, std::string tableName, common::table_id_t tableID,
        std::unique_ptr<BoundExtraAlterInfo> extraInfo)
        : alterType{alterType}, tableName{std::move(tableName)}, tableID{tableID},
          extraInfo{std::move(extraInfo)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundAlterInfo);

private:
    BoundAlterInfo(const BoundAlterInfo& other)
        : alterType{other.alterType}, tableName{other.tableName}, tableID{other.tableID},
          extraInfo{other.extraInfo->copy()} {}
};

struct BoundExtraRenameTableInfo : public BoundExtraAlterInfo {
    std::string newName;

    explicit BoundExtraRenameTableInfo(std::string newName) : newName{std::move(newName)} {}
    BoundExtraRenameTableInfo(const BoundExtraRenameTableInfo& other) : newName{other.newName} {}

    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraRenameTableInfo>(*this);
    }
};

struct BoundExtraAddPropertyInfo : public BoundExtraAlterInfo {
    std::string propertyName;
    common::LogicalType dataType;
    std::unique_ptr<parser::ParsedExpression> defaultValue;
    std::shared_ptr<Expression> boundDefault;

    BoundExtraAddPropertyInfo(std::string propertyName, common::LogicalType dataType,
        std::unique_ptr<parser::ParsedExpression> defaultValue,
        std::shared_ptr<Expression> boundDefault)
        : propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValue{std::move(defaultValue)}, boundDefault{std::move(boundDefault)} {}
    BoundExtraAddPropertyInfo(const BoundExtraAddPropertyInfo& other)
        : propertyName{other.propertyName}, dataType{other.dataType.copy()},
          defaultValue{other.defaultValue->copy()}, boundDefault{other.boundDefault} {}

    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraAddPropertyInfo>(*this);
    }
};

struct BoundExtraDropPropertyInfo : public BoundExtraAlterInfo {
    common::property_id_t propertyID;
    std::string propertyName;

    explicit BoundExtraDropPropertyInfo(common::property_id_t propertyID, std::string propertyName)
        : propertyID{propertyID}, propertyName{propertyName} {}
    BoundExtraDropPropertyInfo(const BoundExtraDropPropertyInfo& other)
        : propertyID{other.propertyID}, propertyName{other.propertyName} {}

    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraDropPropertyInfo>(*this);
    }
};

struct BoundExtraRenamePropertyInfo : public BoundExtraAlterInfo {
    common::property_id_t propertyID;
    std::string newName;
    std::string oldName;

    BoundExtraRenamePropertyInfo(common::property_id_t propertyID, std::string newName,
        std::string oldName)
        : propertyID{propertyID}, newName{std::move(newName)}, oldName{std::move(oldName)} {}
    BoundExtraRenamePropertyInfo(const BoundExtraRenamePropertyInfo& other)
        : propertyID{other.propertyID}, newName{other.newName}, oldName{other.oldName} {}
    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraRenamePropertyInfo>(*this);
    }
};

struct BoundExtraCommentInfo : public BoundExtraAlterInfo {
    std::string comment;

    explicit BoundExtraCommentInfo(std::string comment) : comment{std::move(comment)} {}
    BoundExtraCommentInfo(const BoundExtraCommentInfo& other) : comment{other.comment} {}
    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraCommentInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
