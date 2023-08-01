#pragma once

#include "binder/expression/rel_expression.h"
#include "update_table_type.h"

namespace kuzu {
namespace binder {

struct ExtraCreateInfo {
    virtual ~ExtraCreateInfo() = default;
    virtual std::unique_ptr<ExtraCreateInfo> copy() const = 0;
};

struct ExtraCreateNodeInfo : public ExtraCreateInfo {
    std::shared_ptr<Expression> primaryKey;

    explicit ExtraCreateNodeInfo(std::shared_ptr<Expression> primaryKey)
        : primaryKey{std::move(primaryKey)} {}
    ExtraCreateNodeInfo(const ExtraCreateNodeInfo& other) : primaryKey{other.primaryKey} {}

    inline std::unique_ptr<ExtraCreateInfo> copy() const final {
        return std::make_unique<ExtraCreateNodeInfo>(*this);
    }
};

struct BoundCreateInfo {
    UpdateTableType updateTableType;
    std::shared_ptr<Expression> nodeOrRel;
    std::vector<expression_pair> setItems;
    std::unique_ptr<ExtraCreateInfo> extraInfo;

    BoundCreateInfo(UpdateTableType updateTableType, std::shared_ptr<Expression> nodeOrRel,
        std::vector<expression_pair> setItems, std::unique_ptr<ExtraCreateInfo> extraInfo)
        : updateTableType{updateTableType}, nodeOrRel{std::move(nodeOrRel)},
          setItems{std::move(setItems)}, extraInfo{std::move(extraInfo)} {}
    BoundCreateInfo(const BoundCreateInfo& other)
        : updateTableType{other.updateTableType}, nodeOrRel{other.nodeOrRel}, setItems{
                                                                                  other.setItems} {
        if (other.extraInfo) {
            extraInfo = other.extraInfo->copy();
        }
    }

    inline std::unique_ptr<BoundCreateInfo> copy() {
        return std::make_unique<BoundCreateInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
