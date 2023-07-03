#include "storage/copier/rdf_reader.h"

#include <cstdio>
#include <memory>
#include <utility>

#include "common/constants.h"
#include "common/vector/value_vector.h"
#include "serd.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

RDFReader::RDFReader(std::string filePath, bool strict)
    : filePath{std::move(filePath)}, subjectVector{nullptr}, predicateVector{nullptr},
      objectVector{nullptr}, numLinesPerBatchRead{0}, status{SERD_SUCCESS},
      logger{LoggerUtils::getLogger(common::LoggerConstants::LoggerEnum::RDF_READER)}, strict{
                                                                                           strict} {
    // TODO(gaurav): Add support for fopen in FileUtils and use that here.
    std::string fileName = this->filePath.substr(this->filePath.find_last_of("/\\") + 1);
    fp = fopen(this->filePath.c_str(), "rb");

    struct stat fileStatus {};
    stat(this->filePath.c_str(), &fileStatus);
    fileSize = fileStatus.st_size;

    reader =
        serd_reader_new(SERD_TURTLE, this, nullptr, nullptr, nullptr, handleStatements, nullptr);
    serd_reader_set_strict(reader, strict);
    serd_reader_set_error_sink(reader, errorHandle, this);
    serd_reader_start_stream(reader, fp, reinterpret_cast<const uint8_t*>(fileName.c_str()), true);
}

RDFReader::~RDFReader() {
    serd_reader_free(reader);
    fclose(fp);
}

SerdStatus RDFReader::errorHandle(void* handle, const SerdError* error) {
    auto rdfReader = ((RDFReader*)handle);
    if (!rdfReader->strict && error->status == SERD_ERR_BAD_SYNTAX) {
        rdfReader->logger->warn("Syntax error while reading rdf file at line {} and col {}. "
                                "Skipping this triple since strict mode is disabled.",
            error->line, error->col);
    } else if (error->status != SERD_SUCCESS && error->status != SERD_FAILURE) {
        throw common::CopyException(fmt::format("{} while reading rdf file at line {} and col {}",
            (char*)serd_strerror(error->status), error->line, error->col));
    }
    return error->status;
}

SerdStatus RDFReader::handleStatements(void* handle, SerdStatementFlags flags,
    const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
    const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang) {
    auto rdfReader = ((RDFReader*)handle);

    if (!(isSerdTypeSupported(subject->type) && isSerdTypeSupported(predicate->type) &&
            isSerdTypeSupported(object->type))) {
        return SERD_SUCCESS;
    }

    common::StringVector::addString(rdfReader->subjectVector.get(), rdfReader->numLinesPerBatchRead,
        (char*)subject->buf, subject->n_chars);
    common::StringVector::addString(rdfReader->predicateVector.get(),
        rdfReader->numLinesPerBatchRead, (char*)predicate->buf, predicate->n_chars);
    common::StringVector::addString(rdfReader->objectVector.get(), rdfReader->numLinesPerBatchRead,
        (char*)object->buf, object->n_chars);

    rdfReader->numLinesPerBatchRead++;
    return SERD_SUCCESS;
}

bool RDFReader::isSerdTypeSupported(SerdType serdType) {
    return serdType == SERD_BLANK || serdType == SERD_URI || serdType == SERD_CURIE;
}

offset_t RDFReader::read(const std::shared_ptr<common::ValueVector>& subject,
    const std::shared_ptr<common::ValueVector>& predicate,
    const std::shared_ptr<common::ValueVector>& object) {
    if (status) {
        return 0;
    }

    this->subjectVector = subject;
    this->predicateVector = predicate;
    this->objectVector = object;

    numLinesPerBatchRead = 0;
    while (true) {
        status = serd_reader_read_chunk(reader);
        if (!strict && status == SERD_ERR_BAD_SYNTAX) {
            serd_reader_skip_until_byte(reader, (uint8_t)'\n');
            continue;
        }
        if (status || numLinesPerBatchRead >= DEFAULT_VECTOR_CAPACITY) {
            break;
        }
    }

    return numLinesPerBatchRead;
}

} // namespace storage
} // namespace kuzu
