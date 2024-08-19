#include "processor/warning_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::vector<std::string> WarningSchema::getColumnNames() {
    static constexpr std::initializer_list<const char*> names = {"Message", "File Path",
        "Line Number", "Reconstructed Line"};
    static_assert(getNumColumns() == names.size());
    std::vector<std::string> ret;
    for (const auto name : names) {
        ret.emplace_back(name);
    }
    return ret;
}

std::vector<common::LogicalType> WarningSchema::getColumnDataTypes() {
    std::vector<common::LogicalType> ret;
    ret.push_back(common::LogicalType::STRING());
    ret.push_back(common::LogicalType::STRING());
    ret.push_back(common::LogicalType::UINT64());
    ret.push_back(common::LogicalType::STRING());
    KU_ASSERT(getNumColumns() == ret.size());

    return common::LogicalType::copy(ret);
}

WarningContext::WarningContext(main::ClientContext* clientContext) : clientContext(clientContext) {
    FactorizedTableSchema tableSchema;

    const auto dataTypes = WarningSchema::getColumnDataTypes();
    for (idx_t i = 0; i < WarningSchema::getNumColumns(); ++i) {
        tableSchema.appendColumn(
            ColumnSchema{false, i, LogicalTypeUtils::getRowLayoutSize(dataTypes[i])});
    }

    warningTable = std::make_unique<FactorizedTable>(this->clientContext->getMemoryManager(),
        std::move(tableSchema));
}

namespace {
std::shared_ptr<ValueVector> createValueVector(LogicalTypeID dataType,
    main::ClientContext* clientContext) {
    auto vec = std::make_shared<ValueVector>(dataType, clientContext->getMemoryManager());
    uint64_t vectorSize = DEFAULT_VECTOR_CAPACITY;
    vec->state = std::make_shared<DataChunkState>(vectorSize);
    vec->state->initOriginalAndSelectedSize(vectorSize);
    vec->state->setToFlat();
    return vec;
}

void insertStringToVector(ValueVector* vec, idx_t pos, const std::string& str) {
    auto outputKUStr = ku_string_t();
    outputKUStr.overflowPtr = reinterpret_cast<uint64_t>(
        StringVector::getInMemOverflowBuffer(vec)->allocateSpace(str.length()));
    outputKUStr.set(str);
    vec->setValue(pos, outputKUStr);
}
} // namespace

// marking this function with const would be misleading here
// NOLINTNEXTLINE(readability-make-member-function-const)
void WarningContext::appendWarningMessages(const std::vector<PopulatedCSVError>& messages,
    uint64_t messageLimit) {
    common::UniqLock lock{mtx};
    warningLimit = messageLimit;

    auto messageVector = createValueVector(LogicalTypeID::STRING, clientContext);
    auto pathVector = createValueVector(LogicalTypeID::STRING, clientContext);
    auto lineVector = createValueVector(LogicalTypeID::UINT64, clientContext);
    auto reconstructedLineVector = createValueVector(LogicalTypeID::STRING, clientContext);

    for (idx_t copyStartIdx = 0; copyStartIdx < messages.size(); ++copyStartIdx) {
        for (idx_t copyIdx = copyStartIdx;
             copyIdx < std::min(static_cast<uint64_t>(messages.size()),
                           copyStartIdx + DEFAULT_VECTOR_CAPACITY);
             ++copyIdx) {
            const auto& error = messages[copyIdx];

            const auto copyIdxInVec = copyIdx - copyStartIdx;
            lineVector->setValue(copyIdxInVec, error.lineNumber);
            insertStringToVector(messageVector.get(), copyIdxInVec, error.message);
            insertStringToVector(pathVector.get(), copyIdxInVec, error.filePath);
            insertStringToVector(reconstructedLineVector.get(), copyIdxInVec,
                error.reconstructedLine);
        }
        std::vector<ValueVector*> vecs{{messageVector.get(), pathVector.get(), lineVector.get(),
            reconstructedLineVector.get()}};
        warningTable->append(vecs);
    }
}

} // namespace processor
} // namespace kuzu
