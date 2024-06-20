#include "sample_extension.h"

#include "binder/binder.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "main/client_context.h"
#include "main/database.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace sample {

enum class SampleTableType : uint8_t { NODE = 0, REL = 1 };

class Sample final : public extension::ExtensionClause {
public:
    explicit Sample(std::string tableName)
        : extension::ExtensionClause{SampleClauseHandler{}}, tableName{std::move(tableName)} {};

    std::string getTableName() const { return tableName; }

private:
    std::string tableName;
};

class BoundSample final : public extension::BoundExtensionClause {
public:
    explicit BoundSample(std::string tableName)
        : extension::BoundExtensionClause{SampleClauseHandler{},
              binder::BoundStatementResult::createSingleStringColumnResult()},
          tableName{std::move(tableName)} {};

    std::string getTableName() const { return tableName; }

private:
    std::string tableName;
};

class LogicalSample final : public extension::LogicalExtensionClause {
public:
    explicit LogicalSample(std::string tableName,
        std::shared_ptr<binder::Expression> outputExpression)
        : extension::LogicalExtensionClause{SampleClauseHandler{}}, tableName{std::move(tableName)},
          outputExpression{std::move(outputExpression)} {}

    void computeFactorizedSchema() override {
        createEmptySchema();
        auto groupPos = schema->createGroup();
        schema->insertToGroupAndScope(outputExpression, groupPos);
        schema->setGroupAsSingleState(groupPos);
    }
    void computeFlatSchema() override {
        createEmptySchema();
        schema->createGroup();
        schema->insertToGroupAndScope(outputExpression, 0);
    }

    std::string getExpressionsForPrinting() const override { return "sample extension"; }

    std::string getTableName() const { return tableName; }

    std::shared_ptr<binder::Expression> getOutputExpression() const { return outputExpression; }

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalSample>(tableName, outputExpression);
    }

private:
    std::string tableName;
    std::shared_ptr<binder::Expression> outputExpression;
};

struct SampleInfo {
    std::string tableName;
    processor::DataPos outputPos;
};

class PhysicalSample : public extension::PhysicalExtensionClause {
public:
    PhysicalSample(SampleInfo sampleInfo, uint32_t id,
        std::unique_ptr<processor::OPPrintInfo> printInfo)
        : PhysicalExtensionClause{id, std::move(printInfo)}, sampleInfo{std::move(sampleInfo)} {}

    bool isSource() const override { return true; }
    bool isParallel() const final { return false; }

    void initLocalStateInternal(processor::ResultSet* resultSet,
        processor::ExecutionContext* /*context*/) override {
        outputVector = resultSet->getValueVector(sampleInfo.outputPos).get();
    }

    bool getNextTuplesInternal(processor::ExecutionContext* context) override {
        if (hasExecuted) {
            return false;
        }
        hasExecuted = true;
        std::vector<binder::PropertyInfo> propertyInfos;
        propertyInfos.push_back(binder::PropertyInfo{"name", common::LogicalType::STRING()});
        binder::BoundCreateTableInfo info{common::TableType::NODE, sampleInfo.tableName,
            common::ConflictAction::ON_CONFLICT_THROW,
            std::make_unique<binder::BoundExtraCreateNodeTableInfo>(0, std::move(propertyInfos))};
        auto catalog = context->clientContext->getCatalog();
        auto newTableID = catalog->createTableSchema(context->clientContext->getTx(), info);
        auto storageManager = context->clientContext->getStorageManager();
        storageManager->createTable(newTableID, catalog, context->clientContext);
        outputVector->setValue(0, std::string("New table has been created by extension."));
        outputVector->state->getSelVectorUnsafe().setSelSize(1);
        metrics->numOutputTuple.increase(1);
        return true;
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<PhysicalSample>(sampleInfo, id, printInfo->copy());
    }

private:
    SampleInfo sampleInfo;
    common::ValueVector* outputVector;
    bool hasExecuted = false;
};

void SampleExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();
    db->registerExtensionClauseHandler("sample", std::make_unique<SampleClauseHandler>());
}

static std::vector<std::shared_ptr<parser::Statement>> parseFunction(std::string_view query) {
    auto tableName = std::string(query).substr(std::string(query).find_last_of(' ') + 1);
    if (std::string(query).find("NODE") == std::string::npos) {
        throw common::ParserException{"Invalid query: " + std::string(query)};
    }
    return {std::make_shared<Sample>(tableName)};
}

static std::unique_ptr<binder::BoundStatement> bindFunction(
    const extension::ExtensionClause& statement, const binder::Binder& binder) {
    auto& sample = statement.constCast<Sample>();
    auto context = binder.getClientContext();
    if (context->getCatalog()->containsTable(context->getTx(), sample.getTableName())) {
        throw common::BinderException{
            common::stringFormat("Table {} already exists.", sample.getTableName())};
    }
    return std::make_unique<BoundSample>(sample.getTableName());
}

static void planFunction(planner::LogicalPlan& plan,
    const extension::BoundExtensionClause& statement) {
    auto& boundSample = statement.constCast<BoundSample>();
    auto op = std::make_unique<LogicalSample>(boundSample.getTableName(),
        statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

static std::unique_ptr<processor::PhysicalOperator> mapFunction(
    const extension::LogicalExtensionClause& op, uint32_t operatorID) {
    auto& logicalSample = op.constCast<LogicalSample>();
    auto outSchema = logicalSample.getSchema();
    auto outputExpression = logicalSample.getOutputExpression();
    auto dataPos = processor::DataPos(outSchema->getExpressionPos(*outputExpression));
    SampleInfo sampleInfo{logicalSample.getTableName(), dataPos};
    return std::make_unique<PhysicalSample>(sampleInfo, operatorID,
        std::make_unique<processor::OPPrintInfo>(logicalSample.getExpressionsForPrinting()));
}

static bool defaultReadWriteAnalyzer(const extension::ExtensionClause& /*statement*/) {
    return false;
}

SampleClauseHandler::SampleClauseHandler()
    : extension::ExtensionClauseHandler{parseFunction, bindFunction, planFunction, mapFunction} {
    readWriteAnalyzer = defaultReadWriteAnalyzer;
}

} // namespace sample
} // namespace kuzu

extern "C" {
// Because we link against the static library on windows, we implicitly inherit KUZU_STATIC_DEFINE,
// which cancels out any exporting, so we can't use KUZU_API.
#if defined(_WIN32)
#define INIT_EXPORT __declspec(dllexport)
#else
#define INIT_EXPORT __attribute__((visibility("default")))
#endif
INIT_EXPORT void init(kuzu::main::ClientContext* context) {
    kuzu::sample::SampleExtension::load(context);
}
}
