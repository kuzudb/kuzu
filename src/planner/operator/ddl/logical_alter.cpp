#include "planner/operator/ddl/logical_alter.h"

namespace kuzu {
namespace planner {

std::string LogicalAlterPrintInfo::toString() const {
    std::string result = "Operation: ";
    switch (alterType) {
    case common::AlterType::RENAME_TABLE: {
        auto renameInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraRenameTableInfo*>(info);
        result += "Rename Table " + tableName + " to " + renameInfo->newName;
        break;
    }
    case common::AlterType::ADD_PROPERTY: {
        auto addPropInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraAddPropertyInfo*>(info);
        result += "Add Property " + addPropInfo->propertyName + " to Table " + tableName;
        break;
    }
    case common::AlterType::DROP_PROPERTY: {
        auto dropPropInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraDropPropertyInfo*>(info);
        result += "Drop Property " + dropPropInfo->propertyName + " from Table " + tableName;
        break;
    }
    case common::AlterType::RENAME_PROPERTY: {
        auto renamePropInfo = common::ku_dynamic_cast<binder::BoundExtraAlterInfo*,
            binder::BoundExtraRenamePropertyInfo*>(info);
        result += "Rename Property " + renamePropInfo->oldName + " to " + renamePropInfo->newName +
                  " in Table " + tableName;
        break;
    }
    case common::AlterType::COMMENT: {
        result += "Comment on Table " + tableName;
        break;
    }
    default:
        break;
    }
    return result;
}

} // namespace planner
} // namespace kuzu
