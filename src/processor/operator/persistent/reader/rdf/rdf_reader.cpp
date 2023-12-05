#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

#include <cstdio>

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/keyword/rdf_keyword.h"
#include "common/string_format.h"
#include "common/vector/value_vector.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"
#include "serd.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::common::rdf;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

RdfReader::~RdfReader() {
    serd_reader_end_stream(reader);
    serd_reader_free(reader);
    // Even if the close fails, the stream is in an undefined state. There really isn't anything we
    // can do.
    (void)fclose(fp);
}

static SerdSyntax getSerdSyntax(FileType fileType) {
    switch (fileType) {
    case FileType::TURTLE:
        return SerdSyntax ::SERD_TURTLE;
    case FileType::NQUADS:
        return SerdSyntax ::SERD_NQUADS;
    default:
        KU_UNREACHABLE;
    }
}

void RdfReader::initReader() {
    KU_ASSERT(reader == nullptr);
    fp = fopen(this->filePath.c_str(), "rb");
    SerdStatementSink statementHandle;
    switch (mode) {
    case common::RdfReaderMode::RESOURCE: {
        statementHandle = rHandle;
    } break;
    case common::RdfReaderMode::LITERAL: {
        statementHandle = lHandle;
    } break;
    case common::RdfReaderMode::RESOURCE_TRIPLE: {
        statementHandle = rrrHandle;
    } break;
    case common::RdfReaderMode::LITERAL_TRIPLE: {
        statementHandle = rrlHandle;
    } break;
    default:
        KU_UNREACHABLE;
    }
    reader = serd_reader_new(
        getSerdSyntax(fileType), this, nullptr, nullptr, prefixHandle, statementHandle, nullptr);
    serd_reader_set_strict(reader, false /* strict */);
    serd_reader_set_error_sink(reader, errorHandle, this);
    auto fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
}

SerdStatus RdfReader::errorHandle(void* /*handle*/, const SerdError* error) {
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
    case SERD_URI:
    case SERD_CURIE:
    case SERD_LITERAL:
        return true;
    case SERD_BLANK:
        return false;
    default:
        KU_UNREACHABLE;
    }
}

static bool supportTriple(
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object) {
    return supportSerdType(subject->type) && supportSerdType(predicate->type) &&
           supportSerdType(object->type);
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

SerdStatus RdfReader::rHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    addResourceToVector(reader->sVector, reader->vectorSize++, subject);
    addResourceToVector(reader->sVector, reader->vectorSize++, predicate);
    if (object->type != SERD_LITERAL) {
        addResourceToVector(reader->sVector, reader->vectorSize++, object);
    }
    return SERD_SUCCESS;
}

SerdStatus RdfReader::lHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object,
    const SerdNode* object_datatype, const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    if (object->type != SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    addLiteralToVector(reader->sVector, reader->vectorSize++, object, object_datatype);
    reader->rowOffset++;
    return SERD_SUCCESS;
}

SerdStatus RdfReader::rrrHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportTriple(subject, predicate, object)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    if (object->type == SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    addResourceToVector(reader->sVector, reader->vectorSize, subject);
    addResourceToVector(reader->pVector, reader->vectorSize, predicate);
    addResourceToVector(reader->oVector, reader->vectorSize, object);
    reader->vectorSize++;
    return SERD_SUCCESS;
}

SerdStatus RdfReader::rrlHandle(void* handle, SerdStatementFlags, const SerdNode*,
    const SerdNode* subject, const SerdNode* predicate, const SerdNode* object, const SerdNode*,
    const SerdNode*) {
    if (!supportSerdType(subject->type) || !supportSerdType(predicate->type) ||
        !supportSerdType(object->type)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RdfReader*>(handle);
    if (object->type != SERD_LITERAL) {
        return SERD_SUCCESS;
    }
    addResourceToVector(reader->sVector, reader->vectorSize, subject);
    addResourceToVector(reader->pVector, reader->vectorSize, predicate);
    reader->oVector->setValue<int64_t>(reader->vectorSize, reader->rowOffset);
    reader->vectorSize++;
    reader->rowOffset++;
    return SERD_SUCCESS;
}

SerdStatus RdfReader::prefixHandle(void* handle, const SerdNode* /*name*/, const SerdNode* uri) {
    auto reader = reinterpret_cast<RdfReader*>(handle);
    reader->currentPrefix = reinterpret_cast<const char*>(uri->buf);
    return SERD_SUCCESS;
}

offset_t RdfReader::read(DataChunk* dataChunk) {
    if (status) {
        return 0;
    }

    switch (mode) {
    case RdfReaderMode::RESOURCE_TRIPLE:
    case RdfReaderMode::LITERAL_TRIPLE: {
        sVector = dataChunk->getValueVector(0).get();
        pVector = dataChunk->getValueVector(1).get();
        oVector = dataChunk->getValueVector(2).get();
    } break;
    default: {
        sVector = dataChunk->getValueVector(0).get();
    } break;
    }
    vectorSize = 0;
    while (true) {
        status = serd_reader_read_chunk(reader);
        // See comment below.
        if (vectorSize > DEFAULT_VECTOR_CAPACITY) {
            printf("warning\n");
        }
        if (status == SERD_ERR_BAD_SYNTAX) {
            // Skip to the next line.
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            continue;
        }
        // We cannot control how many rows being read in each serd_reader_read_chunk(). Empirically,
        // a chunk shouldn't be too large. We leave a buffer size 100 to avoid vector size exceed
        // 2048 after reading the last chunk.
        if (status != SERD_SUCCESS || vectorSize + 100 >= DEFAULT_VECTOR_CAPACITY) {
            break;
        }
    }
    dataChunk->state->selVector->selectedSize = vectorSize;
    return vectorSize;
}

void RdfScanSharedState::read(common::DataChunk& dataChunk) {
    std::lock_guard<std::mutex> mtx{lock};
    do {
        if (fileIdx > readerConfig.getNumFiles()) {
            return;
        }
        if (reader->read(&dataChunk) > 0) {
            return;
        }
        fileIdx++;
        initReader();
    } while (true);
}

void RdfScanSharedState::initReader() {
    if (fileIdx >= readerConfig.getNumFiles()) {
        return;
    }
    auto path = readerConfig.filePaths[fileIdx];
    auto rdfConfig = reinterpret_cast<RdfReaderConfig*>(readerConfig.extraConfig.get());
    reader = std::make_unique<RdfReader>(path, readerConfig.fileType, *rdfConfig);
    reader->initReader();
}

function::function_set RdfScan::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(READ_RDF_FUNC_NAME, tableFunc, bindFunc,
        initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING});
    functionSet.push_back(std::move(func));
    return functionSet;
}

void RdfScan::tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto sharedState = reinterpret_cast<RdfScanSharedState*>(input.sharedState);
    sharedState->read(outputChunk);
}

std::unique_ptr<function::TableFuncBindData> RdfScan::bindFunc(main::ClientContext* /*context*/,
    function::TableFuncBindInput* input, catalog::Catalog* /*catalog*/,
    storage::StorageManager* /*storageManager*/) {
    auto scanInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    return std::make_unique<function::ScanBindData>(
        common::LogicalType::copy(scanInput->expectedColumnTypes), scanInput->expectedColumnNames,
        scanInput->mm, scanInput->config);
}

std::unique_ptr<function::TableFuncSharedState> RdfScan::initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    return std::make_unique<RdfScanSharedState>(bindData->config, 0 /* numRows */);
}

std::unique_ptr<function::TableFuncLocalState> RdfScan::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/,
    storage::MemoryManager* /*mm*/) {
    return std::make_unique<TableFuncLocalState>();
}

} // namespace processor
} // namespace kuzu
