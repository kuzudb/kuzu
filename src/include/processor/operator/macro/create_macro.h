#pragma once

#include "catalog/catalog.h"
#include "function/scalar_macro_function.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct CreateMacroInfo {
    std::string macroName;
    std::unique_ptr<function::ScalarMacroFunction> macro;
    DataPos outputPos;
    catalog::Catalog* catalog;

    CreateMacroInfo(std::string macroName, std::unique_ptr<function::ScalarMacroFunction> macro,
        DataPos outputPos, catalog::Catalog* catalog)
        : macroName{std::move(macroName)}, macro{std::move(macro)}, outputPos{outputPos},
          catalog{catalog} {}

    inline std::unique_ptr<CreateMacroInfo> copy() {
        return std::make_unique<CreateMacroInfo>(macroName, macro->copy(), outputPos, catalog);
    }
};

struct CreateMacroPrintInfo final : OPPrintInfo {
    std::string macroName;

    explicit CreateMacroPrintInfo(std::string macroName) : macroName{std::move(macroName)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<CreateMacroPrintInfo>(new CreateMacroPrintInfo(*this));
    }

private:
    CreateMacroPrintInfo(const CreateMacroPrintInfo& other)
        : OPPrintInfo{other}, macroName{other.macroName} {}
};

class CreateMacro : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::CREATE_MACRO;

public:
    CreateMacro(std::unique_ptr<CreateMacroInfo> createMacroInfo, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, id, std::move(printInfo)},
          createMacroInfo{std::move(createMacroInfo)}, outputVector(nullptr) {}

    inline bool isSource() const override { return true; }
    inline bool isParallel() const final { return false; }

    inline void initLocalStateInternal(ResultSet* resultSet,
        ExecutionContext* /*context*/) override {
        outputVector = resultSet->getValueVector(createMacroInfo->outputPos).get();
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CreateMacro>(createMacroInfo->copy(), id, printInfo->copy());
    }

private:
    std::unique_ptr<CreateMacroInfo> createMacroInfo;
    bool hasExecuted = false;
    common::ValueVector* outputVector;
};

} // namespace processor
} // namespace kuzu
