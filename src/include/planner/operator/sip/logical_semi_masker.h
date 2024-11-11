#pragma once

#include "common/enums/extend_direction.h"
#include "common/exception/runtime.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

/*
 * NODE
 * offsets are collected from value vector of type NODE_ID (INTERNAL_ID)
 *
 * PATH
 * offsets are collected from value vector of type PATH (LIST[INTERNAL_ID]). This is a fast-path
 * code used when scanning properties along the path.
 *
 * */
enum class SemiMaskKeyType : uint8_t {
    NODE = 0,
    PATH = 1,
    NODE_ID_LIST = 2,
};

enum class SemiMaskTargetType : uint8_t {
    SCAN_NODE = 0,
    RECURSIVE_JOIN_TARGET_NODE = 1,
    GDS_INPUT_NODE = 2,
    GDS_PATH_NODE = 3,
    GDS_OUTPUT_NODE = 4,
};

struct ExtraKeyInfo {
    virtual ~ExtraKeyInfo() = default;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

    virtual std::unique_ptr<ExtraKeyInfo> copy() const = 0;
};

struct ExtraPathKeyInfo : public ExtraKeyInfo {
    common::ExtendDirection direction;

    explicit ExtraPathKeyInfo(common::ExtendDirection direction) : direction{direction} {}

    std::unique_ptr<ExtraKeyInfo> copy() const override {
        return std::make_unique<ExtraPathKeyInfo>(direction);
    }
};

struct ExtraNodeIDListKeyInfo : public ExtraKeyInfo {
    std::shared_ptr<binder::Expression> srcNodeID;
    std::shared_ptr<binder::Expression> dstNodeID;

    ExtraNodeIDListKeyInfo(std::shared_ptr<binder::Expression> srcNodeID,
        std::shared_ptr<binder::Expression> dstNodeID)
        : srcNodeID{srcNodeID}, dstNodeID{dstNodeID} {}

    std::unique_ptr<ExtraKeyInfo> copy() const override {
        return std::make_unique<ExtraNodeIDListKeyInfo>(srcNodeID, dstNodeID);
    }
};

class LogicalSemiMasker : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::SEMI_MASKER;

public:
    // Constructor that does not specify operators accepting semi masks. Later stage there must be
    // logics filling "targetOps" field.
    LogicalSemiMasker(SemiMaskKeyType keyType, SemiMaskTargetType targetType,
        std::shared_ptr<binder::Expression> key, std::vector<common::table_id_t> nodeTableIDs,
        std::shared_ptr<LogicalOperator> child)
        : LogicalSemiMasker{keyType, targetType, key, nodeTableIDs, std::vector<LogicalOperator*>{},
              std::move(child)} {}
    LogicalSemiMasker(SemiMaskKeyType keyType, SemiMaskTargetType targetType,
        std::shared_ptr<binder::Expression> key, std::vector<common::table_id_t> nodeTableIDs,
        std::vector<LogicalOperator*> ops, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{type_, std::move(child)}, keyType{keyType}, targetType{targetType},
          key{std::move(key)}, nodeTableIDs{std::move(nodeTableIDs)}, targetOps{std::move(ops)} {}

    void computeFactorizedSchema() override { copyChildSchema(0); }
    void computeFlatSchema() override { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override { return key->toString(); }

    SemiMaskKeyType getKeyType() const { return keyType; }

    SemiMaskTargetType getTargetType() const { return targetType; }

    std::shared_ptr<binder::Expression> getKey() const { return key; }
    void setExtraKeyInfo(std::unique_ptr<ExtraKeyInfo> extraInfo) {
        extraKeyInfo = std::move(extraInfo);
    }
    ExtraKeyInfo* getExtraKeyInfo() const { return extraKeyInfo.get(); }

    std::vector<common::table_id_t> getNodeTableIDs() const { return nodeTableIDs; }

    void addTarget(LogicalOperator* op) { targetOps.push_back(op); }
    std::vector<LogicalOperator*> getTargetOperators() const { return targetOps; }

    std::unique_ptr<LogicalOperator> copy() override {
        if (!targetOps.empty()) {
            throw common::RuntimeException(
                "LogicalSemiMasker::copy() should not be called when ops "
                "is not empty. Raw pointers will be point to corrupted object after copy.");
        }
        auto result = std::make_unique<LogicalSemiMasker>(keyType, targetType, key, nodeTableIDs,
            children[0]->copy());
        if (extraKeyInfo != nullptr) {
            result->setExtraKeyInfo(extraKeyInfo->copy());
        }
        return result;
    }

private:
    SemiMaskKeyType keyType;
    SemiMaskTargetType targetType;
    std::shared_ptr<binder::Expression> key;
    std::unique_ptr<ExtraKeyInfo> extraKeyInfo = nullptr;
    std::vector<common::table_id_t> nodeTableIDs;
    // Operators accepting semi masker
    std::vector<LogicalOperator*> targetOps;
};

} // namespace planner
} // namespace kuzu
