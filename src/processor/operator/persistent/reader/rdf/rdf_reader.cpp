#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

#include <cstdio>

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/string_format.h"
#include "common/vector/value_vector.h"
#include "serd.h"
#include "storage/index/hash_index.h"
#include "storage/store/table_copy_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

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
    sOffsetVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
    pOffsetVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
    oOffsetVector = std::make_unique<ValueVector>(LogicalTypeID::INT64);
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

//<<<<<<< HEAD
// SerdStatus RDFReader::handleStatements(void* handle, SerdStatementFlags /*flags*/,
//    const SerdNode* /*graph*/, const SerdNode* subject, const SerdNode* predicate,
//    const SerdNode* object, const SerdNode* /*object_datatype*/, const SerdNode* /*object_lang*/)
//    { auto rdfReader = reinterpret_cast<RDFReader*>(handle);
//
//    if (!(isSerdTypeSupported(subject->type) && isSerdTypeSupported(predicate->type) &&
//            isSerdTypeSupported(object->type))) {
//=======
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

static void addSerdNodeToVector(
    common::ValueVector* vector, uint32_t vectorPos, const SerdNode* node) {
    StringVector::addString(
        vector, vectorPos, reinterpret_cast<const char*>(node->buf), node->n_bytes);
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
    const SerdNode* object, const SerdNode* /*object_datatype*/, const SerdNode* /*object_lang*/) {
    if (!supportSerdType(subject->type) || !supportSerdType(predicate->type) ||
        !supportSerdType(object->type)) {
        return SERD_SUCCESS;
    }
    auto reader = reinterpret_cast<RDFReader*>(handle);

    switch (reader->config->mode) {
    case RdfReaderMode::RESOURCE: {
        addSerdNodeToVector(reader->sVector, reader->vectorSize++, subject);
        addSerdNodeToVector(reader->sVector, reader->vectorSize++, predicate);
        if (object->type != SERD_LITERAL) {
            addSerdNodeToVector(reader->sVector, reader->vectorSize++, object);
        }
        reader->rowOffset++;
    } break;
    case RdfReaderMode::LITERAL: {
        if (object->type == SERD_LITERAL) {
            addSerdNodeToVector(reader->sVector, reader->vectorSize++, object);
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
        reader->sOffsetVector->setValue<int64_t>(reader->rowOffset, subjectOffset);
        reader->pOffsetVector->setValue<int64_t>(reader->rowOffset, predicateOffset);
        reader->oOffsetVector->setValue<int64_t>(reader->rowOffset, objectOffset);
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
        reader->sOffsetVector->setValue<int64_t>(reader->rowOffset, subjectOffset);
        reader->pOffsetVector->setValue<int64_t>(reader->rowOffset, predicateOffset);
        reader->oOffsetVector->setValue<int64_t>(reader->rowOffset, objectOffset);
        reader->vectorSize++;
        reader->rowOffset++;
    } break;
        // LCOV_EXCL_START
    default:
        throw common::NotImplementedException("RDFReader::statementSink");
        // LCOV_EXCL_STOP
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

static void setArrowVector(
    common::ValueVector* vector, common::ValueVector* offsetVector, uint32_t size) {
    std::shared_ptr<arrow::Buffer> arrowBuffer;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        arrow::AllocateBuffer((int64_t)(size * sizeof(offset_t))).Value(&arrowBuffer));
    auto offsets = (offset_t*)arrowBuffer->data();
    for (auto i = 0u; i < size; ++i) {
        offsets[i] = offsetVector->getValue<int64_t>(i);
    }
    arrow::ArrayVector offsetArraysVector;
    offsetArraysVector.push_back(TableCopyUtils::createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), arrowBuffer, size));
    auto offsetChunkedArray = std::make_shared<arrow::ChunkedArray>(offsetArraysVector);
    ArrowColumnVector::setArrowColumn(vector, offsetChunkedArray);
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
    switch (config->mode) {
    case common::RdfReaderMode::RESOURCE_TRIPLE:
    case common::RdfReaderMode::LITERAL_TRIPLE: {
        setArrowVector(sVector, sOffsetVector.get(), vectorSize);
        setArrowVector(pVector, pOffsetVector.get(), vectorSize);
        setArrowVector(oVector, oOffsetVector.get(), vectorSize);
    } break;
    default:
        break;
    }
    return rowOffset;
}

} // namespace processor
} // namespace kuzu
