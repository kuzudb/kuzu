#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

#include <cstdio>

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/keyword/rdf_keyword.h"
#include "common/string_format.h"
#include "common/vector/value_vector.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"
#include "function/table_functions/scan_functions.h"
#include "serd.h"
#include "storage/index/hash_index.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::common::rdf;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

RDFReader::RDFReader(std::string filePath, std::unique_ptr<common::RdfReaderConfig> config)
    : filePath{std::move(filePath)}, config{std::move(config)}, rowOffset{0},
      vectorSize{0}, sVector{nullptr}, pVector{nullptr}, oVector{nullptr}, status{SERD_SUCCESS} {
    std::string fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    fp = fopen(this->filePath.c_str(), "rb");
    reader = serd_reader_new(
        SERD_TURTLE, this, nullptr, nullptr, prefixSink, readerStatementSink, nullptr);
    serd_reader_set_strict(reader, false /* strict */);
    serd_reader_set_error_sink(reader, errorHandle, this);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
    counter = serd_reader_new(
        SERD_TURTLE, this, nullptr, nullptr, nullptr, counterStatementSink, nullptr);
    serd_reader_set_strict(counter, false /* strict */);
    serd_reader_set_error_sink(counter, errorHandle, this);
    serd_reader_start_stream(counter, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
}

RDFReader::~RDFReader() {
    serd_reader_end_stream(reader);
    serd_reader_free(reader);
    serd_reader_end_stream(counter);
    serd_reader_free(counter);
    fclose(fp);
}

SerdStatus RDFReader::errorHandle(void* /*handle*/, const SerdError* error) {
    if (error->status == SERD_ERR_BAD_SYNTAX) {
        return error->status;
    }
    if (error->status != SERD_SUCCESS && error->status != SERD_FAILURE) {
        throw common::CopyException(
            common::stringFormat("{} while reading rdf file at line {} and col {}",
                (char*)serd_strerror(error->status), error->line, error->col));
    }
    return error->status;
}

static bool supportSerdType(SerdType type) {
    switch (type) {
    case SERD_BLANK:
    case SERD_URI:
    case SERD_CURIE:
    case SERD_LITERAL:
        return true;
    default:
        return false;
    }
}

static void addResourceToVector(
    common::ValueVector* vector, uint32_t vectorPos, const SerdNode* resourceNode) {
    StringVector::addString(
        vector, vectorPos, reinterpret_cast<const char*>(resourceNode->buf), resourceNode->n_bytes);
}

template<typename T>
static void addVariant(ValueVector* vector, uint32_t pos, LogicalTypeID typeID, T val) {
    auto typeVector = StructVector::getFieldVector(vector, 0).get();
    auto valVector = StructVector::getFieldVector(vector, 1).get();
    typeVector->setValue<uint8_t>(pos, static_cast<uint8_t>(typeID));
    BlobVector::addBlob(valVector, pos, (uint8_t*)&val, sizeof(T));
}
static void addVariant(ValueVector* vector, uint32_t pos, const char* src, uint32_t length) {
    auto typeVector = StructVector::getFieldVector(vector, 0).get();
    auto valVector = StructVector::getFieldVector(vector, 1).get();
    typeVector->setValue<uint8_t>(pos, static_cast<uint8_t>(LogicalTypeID::STRING));
    BlobVector::addBlob(valVector, pos, src, length);
}

static void addLiteralToVector(common::ValueVector* vector, uint32_t pos,
    const SerdNode* literalNode, const SerdNode* typeNode) {
    auto data = reinterpret_cast<const char*>(literalNode->buf);
    auto length = literalNode->n_bytes;
    if (typeNode == nullptr) { // No type information available. Treat as string.
        addVariant(vector, pos, data, length);
        return;
    }
    bool resolveAsString = true;
    auto type = std::string_view(reinterpret_cast<const char*>(typeNode->buf), typeNode->n_bytes);
    if (type.starts_with(XSD)) {
        if (type.ends_with(XSD_integer)) {
            // XSD:integer
            int64_t result;
            if (function::trySimpleIntegerCast<int64_t>(data, length, result)) {
                addVariant<int64_t>(vector, pos, LogicalTypeID::INT64, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_double) || type.ends_with(XSD_decimal)) {
            // XSD:double or XSD:decimal
            double_t result;
            if (function::tryDoubleCast(data, length, result)) {
                addVariant<double_t>(vector, pos, LogicalTypeID::DOUBLE, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_boolean)) {
            // XSD:bool
            bool result;
            if (function::tryCastToBool(data, length, result)) {
                addVariant<bool>(vector, pos, LogicalTypeID::BOOL, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_date)) {
            // XSD:date
            date_t result;
            uint64_t dummy = 0;
            if (Date::tryConvertDate(data, length, dummy, result)) {
                addVariant<date_t>(vector, pos, LogicalTypeID::DATE, result);
                resolveAsString = false;
            }
        }
    }
    if (resolveAsString) {
        addVariant(vector, pos, data, length);
    }
}

static common::offset_t lookupResourceNode(PrimaryKeyIndex* index, const SerdNode* node) {
    common::offset_t offset;
    if (!index->lookup(&transaction::DUMMY_READ_TRANSACTION,
            reinterpret_cast<const char*>(node->buf), offset)) {
        throw RuntimeException("resource not found");
    }
    return offset;
}

SerdStatus RDFReader::readerStatementSink(void* handle, SerdStatementFlags /*flags*/,
    const SerdNode* /*graph*/, const SerdNode* subject, const SerdNode* predicate,
    const SerdNode* object, const SerdNode* object_datatype, const SerdNode* /*object_lang*/) {
    if (!supportSerdType(subject->type) || !supportSerdType(predicate->type) ||
        !supportSerdType(object->type)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RDFReader*>(handle);

    switch (reader->config->mode) {
    case RdfReaderMode::RESOURCE: {
        addResourceToVector(reader->sVector, reader->vectorSize++, subject);
        addResourceToVector(reader->sVector, reader->vectorSize++, predicate);
        if (object->type != SERD_LITERAL) {
            addResourceToVector(reader->sVector, reader->vectorSize++, object);
        }
        reader->rowOffset++;
    } break;
    case RdfReaderMode::LITERAL: {
        if (object->type == SERD_LITERAL) {
            addLiteralToVector(reader->sVector, reader->vectorSize++, object, object_datatype);
            reader->rowOffset++;
        }
    } break;
    case RdfReaderMode::RESOURCE_TRIPLE: {
        if (object->type == SERD_LITERAL) {
            return SERD_SUCCESS;
        }
        auto subjectOffset = lookupResourceNode(reader->config->index, subject);
        auto predicateOffset = lookupResourceNode(reader->config->index, predicate);
        auto objectOffset = lookupResourceNode(reader->config->index, object);
        reader->sVector->setValue<int64_t>(reader->rowOffset, subjectOffset);
        reader->pVector->setValue<int64_t>(reader->rowOffset, predicateOffset);
        reader->oVector->setValue<int64_t>(reader->rowOffset, objectOffset);
        reader->vectorSize++;
        reader->rowOffset++;
    } break;
    case RdfReaderMode::LITERAL_TRIPLE: {
        if (object->type != SERD_LITERAL) {
            return SERD_SUCCESS;
        }
        auto subjectOffset = lookupResourceNode(reader->config->index, subject);
        auto predicateOffset = lookupResourceNode(reader->config->index, predicate);
        auto objectOffset = reader->rowOffset;
        reader->sVector->setValue<int64_t>(reader->rowOffset, subjectOffset);
        reader->pVector->setValue<int64_t>(reader->rowOffset, predicateOffset);
        reader->oVector->setValue<int64_t>(reader->rowOffset, objectOffset);
        reader->vectorSize++;
        reader->rowOffset++;
    } break;
    default:
        KU_UNREACHABLE;
    }

    return SERD_SUCCESS;
}

SerdStatus RDFReader::counterStatementSink(void* handle, SerdStatementFlags /*flags*/,
    const SerdNode* /*graph*/, const SerdNode* subject, const SerdNode* predicate,
    const SerdNode* object, const SerdNode* /*object_datatype*/, const SerdNode* /*object_lang*/) {
    if (!supportSerdType(subject->type) || !supportSerdType(predicate->type) ||
        !supportSerdType(object->type)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RDFReader*>(handle);
    reader->rowOffset++;
    return SERD_SUCCESS;
}

SerdStatus RDFReader::prefixSink(void* handle, const SerdNode* /*name*/, const SerdNode* uri) {
    auto reader = reinterpret_cast<RDFReader*>(handle);
    reader->currentPrefix = reinterpret_cast<const char*>(uri->buf);
    return SERD_SUCCESS;
}

common::offset_t RDFReader::countLine() {
    if (status) {
        return 0;
    }
    rowOffset = 0;
    while (true) {
        status = serd_reader_read_chunk(counter);
        if (status == SERD_ERR_BAD_SYNTAX) {
            // Skip to the next line.
            serd_reader_skip_until_byte(counter, (uint8_t)'\n');
            continue;
        }
        if (status != SERD_SUCCESS) {
            break;
        }
    }
    return rowOffset;
}

offset_t RDFReader::read(DataChunk* dataChunk) {
    if (status) {
        return 0;
    }

    switch (config->mode) {
    case common::RdfReaderMode::RESOURCE_TRIPLE:
    case common::RdfReaderMode::LITERAL_TRIPLE: {
        sVector = dataChunk->getValueVector(0).get();
        pVector = dataChunk->getValueVector(1).get();
        oVector = dataChunk->getValueVector(2).get();
    } break;
    default: {
        sVector = dataChunk->getValueVector(0).get();
    } break;
    }
    vectorSize = 0;
    rowOffset = 0;
    while (true) {
        status = serd_reader_read_chunk(reader);
        if (status == SERD_ERR_BAD_SYNTAX) {
            // Skip to the next line.
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            continue;
        }
        // Each line from turtle will increase vector size by 3. To avoid exceeding
        // DEFAULT_VECTOR_CAPACITY, we check the size by adding 3 first.
        if (status != SERD_SUCCESS || vectorSize + 3 >= DEFAULT_VECTOR_CAPACITY) {
            break;
        }
    }
    dataChunk->state->selVector->selectedSize = vectorSize;
    return rowOffset;
}

function::function_set RdfScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_FUNC_NAME, tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    func->canParallelFunc = [] { return false; };
    functionSet.push_back(std::move(func));
    return functionSet;
}

void RdfScan::tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto localState = reinterpret_cast<RdfScanLocalState*>(input.localState);
    localState->reader->read(&outputChunk);
}

std::unique_ptr<function::TableFuncBindData> RdfScan::bindFunc(main::ClientContext* /*context*/,
    function::TableFuncBindInput* input, catalog::CatalogContent* /*catalog*/) {
    auto scanInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    return std::make_unique<function::ScanBindData>(
        common::LogicalType::copy(scanInput->expectedColumnTypes), scanInput->expectedColumnNames,
        scanInput->mm, scanInput->config);
}

std::unique_ptr<function::TableFuncSharedState> RdfScan::initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    auto reader = make_unique<RDFReader>(
        bindData->config.filePaths[0], bindData->config.rdfReaderConfig->copy());
    return std::make_unique<function::ScanSharedState>(bindData->config, reader->countLine());
}

std::unique_ptr<function::TableFuncLocalState> RdfScan::initLocalState(
    function::TableFunctionInitInput& input, function::TableFuncSharedState* /*state*/) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    auto localState = std::make_unique<RdfScanLocalState>();
    localState->reader = std::make_unique<RDFReader>(
        bindData->config.filePaths[0], bindData->config.rdfReaderConfig->copy());
    return localState;
}

} // namespace processor
} // namespace kuzu
