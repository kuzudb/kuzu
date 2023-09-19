#include "processor/operator/persistent/reader/rdf_reader.h"

#include <cstdio>

#include "common/constants.h"
#include "common/exception/copy.h"
#include "common/vector/value_vector.h"
#include "serd.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

RDFReader::RDFReader(std::string filePath)
    : filePath{std::move(filePath)}, subjectVector{nullptr}, predicateVector{nullptr},
      objectVector{nullptr}, numLinesRead{0}, status{SERD_SUCCESS} {
    std::string fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    fp = fopen(this->filePath.c_str(), "rb");
    reader =
        serd_reader_new(SERD_TURTLE, this, nullptr, nullptr, nullptr, handleStatements, nullptr);
    serd_reader_set_strict(reader, false /* strict */);
    serd_reader_set_error_sink(reader, errorHandle, this);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
}

RDFReader::~RDFReader() {
    serd_reader_end_stream(reader);
    serd_reader_free(reader);
    fclose(fp);
}

SerdStatus RDFReader::errorHandle(void* handle, const SerdError* error) {
    if (error->status == SERD_ERR_BAD_SYNTAX) {
        return error->status;
    }
    if (error->status != SERD_SUCCESS && error->status != SERD_FAILURE) {
        throw common::CopyException(
            common::StringUtils::string_format("{} while reading rdf file at line {} and col {}",
                (char*)serd_strerror(error->status), error->line, error->col));
    }
    return error->status;
}

SerdStatus RDFReader::handleStatements(void* handle, SerdStatementFlags flags,
    const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
    const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang) {
    auto rdfReader = reinterpret_cast<RDFReader*>(handle);

    if (!(isSerdTypeSupported(subject->type) && isSerdTypeSupported(predicate->type) &&
            isSerdTypeSupported(object->type))) {
        return SERD_SUCCESS;
    }

    StringVector::addString(rdfReader->subjectVector, rdfReader->numLinesRead,
        reinterpret_cast<const char*>(subject->buf), subject->n_chars);
    StringVector::addString(rdfReader->predicateVector, rdfReader->numLinesRead,
        reinterpret_cast<const char*>(predicate->buf), predicate->n_chars);
    StringVector::addString(rdfReader->objectVector, rdfReader->numLinesRead,
        reinterpret_cast<const char*>(object->buf), object->n_chars);
    rdfReader->numLinesRead++;
    return SERD_SUCCESS;
}

bool RDFReader::isSerdTypeSupported(SerdType serdType) {
    return serdType == SERD_BLANK || serdType == SERD_URI || serdType == SERD_CURIE;
}

offset_t RDFReader::read(DataChunk* dataChunk) {
    if (status) {
        return 0;
    }

    this->subjectVector = dataChunk->getValueVector(0).get();
    this->predicateVector = dataChunk->getValueVector(1).get();
    this->objectVector = dataChunk->getValueVector(2).get();

    numLinesRead = 0;
    while (true) {
        status = serd_reader_read_chunk(reader);
        if (status == SERD_ERR_BAD_SYNTAX) {
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            continue;
        }
        if (status != SERD_SUCCESS || numLinesRead >= DEFAULT_VECTOR_CAPACITY) {
            break;
        }
    }
    dataChunk->state->selVector->selectedSize = numLinesRead;
    return numLinesRead;
}

} // namespace processor
} // namespace kuzu
