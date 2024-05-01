#pragma once

#include "binder/expression/expression.h"
#include "common/enums/alter_type.h"

namespace kuzu {
namespace binder {

struct BoundExtraAlterInfo {
    virtual ~BoundExtraAlterInfo() = default;

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const BoundExtraAlterInfo*, const TARGET*>(this);
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
    std::shared_ptr<Expression> defaultValue;

    BoundExtraAddPropertyInfo(std::string propertyName, common::LogicalType dataType,
        std::shared_ptr<Expression> defaultValue)
        : propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValue{std::move(defaultValue)} {}
    BoundExtraAddPropertyInfo(const BoundExtraAddPropertyInfo& other)
        : propertyName{other.propertyName}, dataType{other.dataType},
          defaultValue{other.defaultValue} {}

    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraAddPropertyInfo>(*this);
    }
};

struct BoundExtraDropPropertyInfo : public BoundExtraAlterInfo {
    common::property_id_t propertyID;

    explicit BoundExtraDropPropertyInfo(common::property_id_t propertyID)
        : propertyID{propertyID} {}
    BoundExtraDropPropertyInfo(const BoundExtraDropPropertyInfo& other)
        : propertyID{other.propertyID} {}

    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraDropPropertyInfo>(*this);
    }
};

struct BoundExtraRenamePropertyInfo : public BoundExtraAlterInfo {
    common::property_id_t propertyID;
    std::string newName;

    BoundExtraRenamePropertyInfo(common::property_id_t propertyID, std::string newName)
        : propertyID{propertyID}, newName{std::move(newName)} {}
    BoundExtraRenamePropertyInfo(const BoundExtraRenamePropertyInfo& other)
        : propertyID{other.propertyID}, newName{other.newName} {}
    inline std::unique_ptr<BoundExtraAlterInfo> copy() const final {
        return std::make_unique<BoundExtraRenamePropertyInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
