#pragma once

#include "binder/expression/rel_expression.h"
#include "update_table_type.h"

namespace kuzu {
namespace binder {

struct ExtraDeleteInfo {
    virtual ~ExtraDeleteInfo() = default;
    virtual std::unique_ptr<ExtraDeleteInfo> copy() const = 0;
};

struct ExtraDeleteNodeInfo : public ExtraDeleteInfo {
    std::shared_ptr<Expression> primaryKey;

    explicit ExtraDeleteNodeInfo(std::shared_ptr<Expression> primaryKey)
        : primaryKey{std::move(primaryKey)} {}
    ExtraDeleteNodeInfo(const ExtraDeleteNodeInfo& other) : primaryKey{other.primaryKey} {}

    inline std::unique_ptr<ExtraDeleteInfo> copy() const final {
        return std::make_unique<ExtraDeleteNodeInfo>(*this);
    }
};

struct BoundDeleteInfo {
    UpdateTableType updateTableType;
    std::shared_ptr<Expression> nodeOrRel;
    std::unique_ptr<ExtraDeleteInfo> extraInfo;

    BoundDeleteInfo(UpdateTableType updateTableType, std::shared_ptr<Expression> nodeOrRel,
        std::unique_ptr<ExtraDeleteInfo> extraInfo)
        : updateTableType{updateTableType}, nodeOrRel{std::move(nodeOrRel)}, extraInfo{std::move(
                                                                                 extraInfo)} {}
    BoundDeleteInfo(const BoundDeleteInfo& other)
        : updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel} {
        if (other.extraInfo) {
            extraInfo = other.extraInfo->copy();
        }
    }

    inline std::unique_ptr<BoundDeleteInfo> copy() {
        return std::make_unique<BoundDeleteInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
