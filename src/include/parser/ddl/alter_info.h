#pragma once

#include <string>

#include "common/copy_constructors.h"
#include "common/enums/alter_type.h"
#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

struct ExtraAlterInfo {
    virtual ~ExtraAlterInfo() = default;
};

struct AlterInfo {
    common::AlterType type;
    std::string tableName;
    std::unique_ptr<ExtraAlterInfo> extraInfo;

    AlterInfo(common::AlterType type, std::string tableName,
        std::unique_ptr<ExtraAlterInfo> extraInfo)
        : type{type}, tableName{std::move(tableName)}, extraInfo{std::move(extraInfo)} {}
    DELETE_COPY_DEFAULT_MOVE(AlterInfo);
};

struct ExtraRenameTableInfo : public ExtraAlterInfo {
    std::string newName;

    explicit ExtraRenameTableInfo(std::string newName) : newName{std::move(newName)} {}
};

struct ExtraAddPropertyInfo : public ExtraAlterInfo {
    std::string propertyName;
    std::string dataType;
    std::unique_ptr<ParsedExpression> defaultValue;

    ExtraAddPropertyInfo(std::string propertyName, std::string dataType,
        std::unique_ptr<ParsedExpression> defaultValue)
        : propertyName{std::move(propertyName)}, dataType{std::move(dataType)},
          defaultValue{std::move(defaultValue)} {}
};

struct ExtraDropPropertyInfo : public ExtraAlterInfo {
    std::string propertyName;

    explicit ExtraDropPropertyInfo(std::string propertyName)
        : propertyName{std::move(propertyName)} {}
};

struct ExtraRenamePropertyInfo : public ExtraAlterInfo {
    std::string propertyName;
    std::string newName;

    ExtraRenamePropertyInfo(std::string propertyName, std::string newName)
        : propertyName{std::move(propertyName)}, newName{std::move(newName)} {}
};

} // namespace parser
} // namespace kuzu
