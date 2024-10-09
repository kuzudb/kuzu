#pragma once

#include "binder/ddl/property_definition.h"
#include "binder/expression/expression.h"
#include "common/enums/alter_type.h"

namespace kuzu {
namespace binder {

struct BoundExtraAlterInfo {
    virtual ~BoundExtraAlterInfo() = default;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const TARGET*>(this);
    }
    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

    virtual std::unique_ptr<BoundExtraAlterInfo> copy() const = 0;
};

struct BoundAlterInfo {
    common::AlterType alterType;
    std::string tableName;
    std::unique_ptr<BoundExtraAlterInfo> extraInfo;

    BoundAlterInfo(common::AlterType alterType, std::string tableName,
        std::unique_ptr<BoundExtraAlterInfo> extraInfo)
        : alterType{alterType}, tableName{std::move(tableName)}, extraInfo{std::move(extraInfo)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundAlterInfo);

    std::string toString() const;

private:
    BoundAlterInfo(const BoundAlterInfo& other)
        : alterType{other.alterType}, tableName{other.tableName},
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
    PropertyDefinition propertyDefinition;
    std::shared_ptr<Expression> boundDefault;

    BoundExtraAddPropertyInfo(const PropertyDefinition& definition,
        std::shared_ptr<Expression> boundDefault)
        : propertyDefinition{definition.copy()}, boundDefault{std::move(boundDefault)} {}
    BoundExtraAddPropertyInfo(const BoundExtraAddPropertyInfo& other)
        : propertyDefinition{other.propertyDefinition.copy()}, boundDefault{other.boundDefault} {}

    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraAddPropertyInfo>(*this);
    }
};

struct BoundExtraDropPropertyInfo : public BoundExtraAlterInfo {
    std::string propertyName;

    explicit BoundExtraDropPropertyInfo(std::string propertyName) : propertyName{propertyName} {}
    BoundExtraDropPropertyInfo(const BoundExtraDropPropertyInfo& other)
        : propertyName{other.propertyName} {}

    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraDropPropertyInfo>(*this);
    }
};

struct BoundExtraRenamePropertyInfo : public BoundExtraAlterInfo {
    std::string newName;
    std::string oldName;

    BoundExtraRenamePropertyInfo(std::string newName, std::string oldName)
        : newName{std::move(newName)}, oldName{std::move(oldName)} {}
    BoundExtraRenamePropertyInfo(const BoundExtraRenamePropertyInfo& other)
        : newName{other.newName}, oldName{other.oldName} {}
    std::unique_ptr<BoundExtraAlterInfo> copy() const final {
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
