// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#include "attributes.h"
#include "byte_sink.h"
#include "serd_internal.h"
#include "stack.h"
#include "string_utils.h"
#include "try.h"
#include "uri_utils.h"

#include "serd.h"

#include <errno.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
  CTX_NAMED, ///< Normal non-anonymous context
  CTX_BLANK, ///< Anonymous blank node
  CTX_LIST,  ///< Anonymous list
} ContextType;

typedef enum {
  FIELD_NONE,
  FIELD_SUBJECT,
  FIELD_PREDICATE,
  FIELD_OBJECT,
  FIELD_GRAPH,
} Field;

typedef struct {
  ContextType type;
  SerdNode    graph;
  SerdNode    subject;
  SerdNode    predicate;
  bool        predicates;
  bool        comma_indented;
} WriteContext;

static const WriteContext WRITE_CONTEXT_NULL = {CTX_NAMED,
                                                {0, 0, 0, 0, SERD_NOTHING},
                                                {0, 0, 0, 0, SERD_NOTHING},
                                                {0, 0, 0, 0, SERD_NOTHING},
                                                0U,
                                                0U};

typedef enum {
  SEP_NONE,        ///< Sentinel before the start of a document
  SEP_NODE,        ///< Sentinel after a node
  SEP_NEWLINE,     ///< Sentinel after a node
  SEP_END_DIRECT,  ///< End of a directive (like "@prefix")
  SEP_END_S,       ///< End of a subject ('.')
  SEP_END_P,       ///< End of a predicate (';')
  SEP_END_O,       ///< End of a named object (',')
  SEP_JOIN_O_AN,   ///< End of anonymous object (',') before a named one
  SEP_JOIN_O_NA,   ///< End of named object (',') before an anonymous one
  SEP_JOIN_O_AA,   ///< End of anonymous object (',') before another
  SEP_S_P,         ///< Between a subject and predicate (whitespace)
  SEP_P_O,         ///< Between a predicate and object (whitespace)
  SEP_ANON_BEGIN,  ///< Start of anonymous node ('[')
  SEP_ANON_S_P,    ///< Between anonymous subject and predicate (whitespace)
  SEP_ANON_END,    ///< End of anonymous node (']')
  SEP_LIST_BEGIN,  ///< Start of list ('(')
  SEP_LIST_SEP,    ///< List separator (whitespace)
  SEP_LIST_END,    ///< End of list (')')
  SEP_GRAPH_BEGIN, ///< Start of graph ('{')
  SEP_GRAPH_END,   ///< End of graph ('}')
} Sep;

typedef uint32_t SepMask; ///< Bitfield of separator flags

typedef struct {
  char    sep;             ///< Sep character
  int     indent;          ///< Indent delta
  SepMask pre_space_after; ///< Leading space if after given seps
  SepMask pre_line_after;  ///< Leading newline if after given seps
  SepMask post_line_after; ///< Trailing newline if after given seps
} SepRule;

#define SEP_EACH (~(SepMask)0)
#define M(s) (1U << (s))
#define NIL '\0'

static const SepRule rules[] = {
  {NIL, +0, SEP_NONE, SEP_NONE, SEP_NONE},
  {NIL, +0, SEP_NONE, SEP_NONE, SEP_NONE},
  {'\n', 0, SEP_NONE, SEP_NONE, SEP_NONE},
  {'.', +0, SEP_EACH, SEP_NONE, SEP_EACH},
  {'.', +0, SEP_EACH, SEP_NONE, SEP_NONE},
  {';', +0, SEP_EACH, SEP_NONE, SEP_EACH},
  {',', +0, SEP_EACH, SEP_NONE, SEP_EACH},
  {',', +0, SEP_EACH, SEP_NONE, SEP_EACH},
  {',', +0, SEP_EACH, SEP_NONE, SEP_EACH},
  {',', +0, SEP_EACH, SEP_NONE, SEP_NONE},
  {NIL, +1, SEP_NONE, SEP_NONE, SEP_EACH},
  {' ', +0, SEP_NONE, SEP_NONE, SEP_NONE},
  {'[', +1, M(SEP_JOIN_O_AA), SEP_NONE, SEP_NONE},
  {NIL, +1, SEP_NONE, SEP_NONE, M(SEP_ANON_BEGIN)},
  {']', -1, SEP_NONE, ~M(SEP_ANON_BEGIN), SEP_NONE},
  {'(', +1, M(SEP_JOIN_O_AA), SEP_NONE, SEP_EACH},
  {NIL, +0, SEP_NONE, SEP_EACH, SEP_NONE},
  {')', -1, SEP_NONE, SEP_EACH, SEP_NONE},
  {'{', +1, SEP_EACH, SEP_NONE, SEP_EACH},
  {'}', -1, SEP_NONE, SEP_NONE, SEP_EACH},
};

#undef NIL
#undef M
#undef SEP_EACH

struct SerdWriterImpl {
  SerdSyntax    syntax;
  SerdStyle     style;
  SerdEnv*      env;
  SerdNode      root_node;
  SerdURI       root_uri;
  SerdURI       base_uri;
  SerdStack     anon_stack;
  SerdByteSink  byte_sink;
  SerdErrorSink error_sink;
  void*         error_handle;
  WriteContext  context;
  uint8_t*      bprefix;
  size_t        bprefix_len;
  Sep           last_sep;
  int           indent;
};

typedef enum { WRITE_STRING, WRITE_LONG_STRING } TextContext;
typedef enum { RESET_GRAPH = 1U << 0U, RESET_INDENT = 1U << 1U } ResetFlag;

SERD_NODISCARD static SerdStatus
write_node(SerdWriter*        writer,
           const SerdNode*    node,
           const SerdNode*    datatype,
           const SerdNode*    lang,
           Field              field,
           SerdStatementFlags flags);

SERD_NODISCARD static bool
supports_abbrev(const SerdWriter* writer)
{
  return writer->syntax == SERD_TURTLE || writer->syntax == SERD_TRIG;
}

SERD_NODISCARD static bool
supports_uriref(const SerdWriter* writer)
{
  return writer->syntax == SERD_TURTLE || writer->syntax == SERD_TRIG;
}

static SerdStatus
free_context(WriteContext* const ctx)
{
  serd_node_free(&ctx->graph);
  serd_node_free(&ctx->subject);
  serd_node_free(&ctx->predicate);
  ctx->graph.type     = SERD_NOTHING;
  ctx->subject.type   = SERD_NOTHING;
  ctx->predicate.type = SERD_NOTHING;
  return SERD_SUCCESS;
}

SERD_LOG_FUNC(3, 4)
static SerdStatus
w_err(SerdWriter* writer, SerdStatus st, const char* fmt, ...)
{
  /* TODO: This results in errors with no file information, which is not
     helpful when re-serializing a file (particularly for "undefined
     namespace prefix" errors.  The statement sink API needs to be changed to
     add a Cursor parameter so the source can notify the writer of the
     statement origin for better error reporting. */

  va_list args; // NOLINT(cppcoreguidelines-init-variables)
  va_start(args, fmt);
  const SerdError e = {st, (const uint8_t*)"", 0, 0, fmt, &args};
  serd_error(writer->error_sink, writer->error_handle, &e);
  va_end(args);
  return st;
}

static void
copy_node(SerdNode* dst, const SerdNode* src)
{
  const size_t   new_size = src->n_bytes + 1U;
  uint8_t* const new_buf  = (uint8_t*)realloc((char*)dst->buf, new_size);
  if (new_buf) {
    dst->buf     = new_buf;
    dst->n_bytes = src->n_bytes;
    dst->n_chars = src->n_chars;
    dst->flags   = src->flags;
    dst->type    = src->type;
    memcpy((char*)dst->buf, src->buf, new_size);
  }
}

static void
push_context(SerdWriter* const writer,
             const ContextType type,
             const SerdNode    graph,
             const SerdNode    subject,
             const SerdNode    predicate)
{
  // Push the current context to the stack
  void* const top = serd_stack_push(&writer->anon_stack, sizeof(WriteContext));
  *(WriteContext*)top = writer->context;

  // Update the current context
  const WriteContext current = {type, graph, subject, predicate, 0U, 0U};
  writer->context            = current;
}

static void
pop_context(SerdWriter* writer)
{
  // Replace the current context with the top of the stack
  free_context(&writer->context);
  writer->context =
    *(WriteContext*)(writer->anon_stack.buf + writer->anon_stack.size -
                     sizeof(WriteContext));

  // Pop the top of the stack away
  serd_stack_pop(&writer->anon_stack, sizeof(WriteContext));
}

SERD_NODISCARD static size_t
sink(const void* buf, size_t len, SerdWriter* writer)
{
  const size_t written = serd_byte_sink_write(buf, len, &writer->byte_sink);
  if (written != len) {
    if (errno) {
      const char* const message = strerror(errno);
      w_err(writer, SERD_ERR_BAD_WRITE, "write error (%s)\n", message);
    } else {
      w_err(writer, SERD_ERR_BAD_WRITE, "write error\n");
    }
  }

  return written;
}

SERD_NODISCARD static inline SerdStatus
esink(const void* buf, size_t len, SerdWriter* writer)
{
  return sink(buf, len, writer) == len ? SERD_SUCCESS : SERD_ERR_BAD_WRITE;
}

// Write a single character, as an escape for single byte characters
// (Caller prints any single byte characters that don't need escaping)
static size_t
write_character(SerdWriter*    writer,
                const uint8_t* utf8,
                size_t*        size,
                SerdStatus*    st)
{
  char           escape[11] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  const uint32_t c          = parse_utf8_char(utf8, size);
  switch (*size) {
  case 0:
    *st =
      w_err(writer, SERD_ERR_BAD_TEXT, "invalid UTF-8 start: %X\n", utf8[0]);
    return 0;
  case 1:
    snprintf(escape, sizeof(escape), "\\u%04X", utf8[0]);
    return sink(escape, 6, writer);
  default:
    break;
  }

  if (!(writer->style & SERD_STYLE_ASCII)) {
    // Write UTF-8 character directly to UTF-8 output
    return sink(utf8, *size, writer);
  }

  if (c <= 0xFFFF) {
    snprintf(escape, sizeof(escape), "\\u%04X", c);
    return sink(escape, 6, writer);
  }

  snprintf(escape, sizeof(escape), "\\U%08X", c);
  return sink(escape, 10, writer);
}

SERD_NODISCARD static bool
uri_must_escape(const uint8_t c)
{
  switch (c) {
  case ' ':
  case '"':
  case '<':
  case '>':
  case '\\':
  case '^':
  case '`':
  case '{':
  case '|':
  case '}':
    return true;
  default:
    return !in_range(c, 0x20, 0x7E);
  }
}

static size_t
write_uri(SerdWriter*    writer,
          const uint8_t* utf8,
          size_t         n_bytes,
          SerdStatus*    st)
{
  size_t len = 0;
  for (size_t i = 0; i < n_bytes;) {
    size_t j = i; // Index of next character that must be escaped
    for (; j < n_bytes; ++j) {
      if (uri_must_escape(utf8[j])) {
        break;
      }
    }

    // Bulk write all characters up to this special one
    const size_t n_bulk = sink(&utf8[i], j - i, writer);
    len += n_bulk;
    if (n_bulk != j - i) {
      *st = SERD_ERR_BAD_WRITE;
      return len;
    }

    if ((i = j) == n_bytes) {
      break; // Reached end
    }

    // Write UTF-8 character
    size_t size = 0;
    len += write_character(writer, utf8 + i, &size, st);
    i += size;
    if (*st && (writer->style & SERD_STYLE_STRICT)) {
      break;
    }

    if (size == 0) {
      // Corrupt input, write percent-encoded bytes and scan to next start
      char escape[4] = {0, 0, 0, 0};
      for (; i < n_bytes && (utf8[i] & 0x80); ++i) {
        snprintf(escape, sizeof(escape), "%%%02X", utf8[i]);
        len += sink(escape, 3, writer);
      }
    }
  }

  return len;
}

SERD_NODISCARD static SerdStatus
ewrite_uri(SerdWriter* writer, const uint8_t* utf8, size_t n_bytes)
{
  SerdStatus st = SERD_SUCCESS;
  write_uri(writer, utf8, n_bytes, &st);

  return (st == SERD_ERR_BAD_WRITE || (writer->style & SERD_STYLE_STRICT))
           ? st
           : SERD_SUCCESS;
}

SERD_NODISCARD static SerdStatus
write_uri_from_node(SerdWriter* writer, const SerdNode* node)
{
  return ewrite_uri(writer, node->buf, node->n_bytes);
}

static bool
lname_must_escape(const uint8_t c)
{
  /* This arbitrary list of characters, most of which have nothing to do with
     Turtle, must be handled as special cases here because the RDF and SPARQL
     WGs are apparently intent on making the once elegant Turtle a baroque
     and inconsistent mess, throwing elegance and extensibility completely
     out the window for no good reason.

     Note '-', '.', and '_' are also in PN_LOCAL_ESC, but are valid unescaped
     in local names, so they are not escaped here. */

  switch (c) {
  case '\'':
  case '!':
  case '#':
  case '$':
  case '%':
  case '&':
  case '(':
  case ')':
  case '*':
  case '+':
  case ',':
  case '/':
  case ';':
  case '=':
  case '?':
  case '@':
  case '~':
    return true;
  default:
    break;
  }
  return false;
}

SERD_NODISCARD static SerdStatus
write_lname(SerdWriter* writer, const uint8_t* utf8, size_t n_bytes)
{
  SerdStatus st = SERD_SUCCESS;
  for (size_t i = 0; i < n_bytes; ++i) {
    size_t j = i; // Index of next character that must be escaped
    for (; j < n_bytes; ++j) {
      if (lname_must_escape(utf8[j])) {
        break;
      }
    }

    // Bulk write all characters up to this special one
    TRY(st, esink(&utf8[i], j - i, writer));
    if ((i = j) == n_bytes) {
      break; // Reached end
    }

    // Write escape
    TRY(st, esink("\\", 1, writer));
    TRY(st, esink(&utf8[i], 1, writer));
  }

  return st;
}

SERD_NODISCARD static SerdStatus
write_text(SerdWriter*    writer,
           TextContext    ctx,
           const uint8_t* utf8,
           size_t         n_bytes)
{
  size_t     n_consecutive_quotes = 0;
  SerdStatus st                   = SERD_SUCCESS;
  for (size_t i = 0; !st && i < n_bytes;) {
    if (utf8[i] != '"') {
      n_consecutive_quotes = 0;
    }

    // Fast bulk write for long strings of printable ASCII
    size_t j = i;
    for (; j < n_bytes; ++j) {
      if (utf8[j] == '\\' || utf8[j] == '"' ||
          (!in_range(utf8[j], 0x20, 0x7E))) {
        break;
      }
    }

    st = esink(&utf8[i], j - i, writer);
    if ((i = j) == n_bytes) {
      break; // Reached end
    }

    const uint8_t in = utf8[i++];
    if (ctx == WRITE_LONG_STRING) {
      n_consecutive_quotes = (in == '\"') ? (n_consecutive_quotes + 1) : 0;

      switch (in) {
      case '\\':
        st = esink("\\\\", 2, writer);
        continue;
      case '\b':
        st = esink("\\b", 2, writer);
        continue;
      case '\n':
      case '\r':
      case '\t':
      case '\f':
        st = esink(&in, 1, writer); // Write character as-is
        continue;
      case '\"':
        if (n_consecutive_quotes >= 3 || i == n_bytes) {
          // Two quotes in a row, or quote at string end, escape
          st = esink("\\\"", 2, writer);
        } else {
          st = esink(&in, 1, writer);
        }
        continue;
      default:
        break;
      }
    } else {
      switch (in) {
      case '\\':
        st = esink("\\\\", 2, writer);
        continue;
      case '\n':
        st = esink("\\n", 2, writer);
        continue;
      case '\r':
        st = esink("\\r", 2, writer);
        continue;
      case '\t':
        st = esink("\\t", 2, writer);
        continue;
      case '"':
        st = esink("\\\"", 2, writer);
        continue;
      default:
        break;
      }
      if (writer->syntax == SERD_TURTLE) {
        switch (in) {
        case '\b':
          st = esink("\\b", 2, writer);
          continue;
        case '\f':
          st = esink("\\f", 2, writer);
          continue;
        default:
          break;
        }
      }
    }

    // Write UTF-8 character
    size_t size = 0;
    write_character(writer, utf8 + i - 1, &size, &st);
    if (st && (writer->style & SERD_STYLE_STRICT)) {
      return st;
    }

    if (size == 0) {
      // Corrupt input, write replacement character and scan to the next start
      st = esink(replacement_char, sizeof(replacement_char), writer);
      for (; i < n_bytes && (utf8[i] & 0x80); ++i) {
      }
    } else {
      i += size - 1;
    }
  }

  return (writer->style & SERD_STYLE_STRICT) ? st : SERD_SUCCESS;
}

typedef struct {
  SerdWriter* writer;
  SerdStatus  status;
} UriSinkContext;

SERD_NODISCARD static size_t
uri_sink(const void* buf, size_t len, void* stream)
{
  UriSinkContext* const context = (UriSinkContext*)stream;
  SerdWriter* const     writer  = context->writer;

  return write_uri(writer, (const uint8_t*)buf, len, &context->status);
}

SERD_NODISCARD static SerdStatus
write_newline(SerdWriter* writer)
{
  SerdStatus st = SERD_SUCCESS;

  TRY(st, esink("\n", 1, writer));
  for (int i = 0; i < writer->indent; ++i) {
    TRY(st, esink("\t", 1, writer));
  }

  return st;
}

SERD_NODISCARD static SerdStatus
write_sep(SerdWriter* writer, const Sep sep)
{
  SerdStatus           st   = SERD_SUCCESS;
  const SepRule* const rule = &rules[sep];

  const bool pre_line  = (rule->pre_line_after & (1U << writer->last_sep));
  const bool post_line = (rule->post_line_after & (1U << writer->last_sep));

  // Adjust indent, but tolerate if it would become negative
  if (rule->indent && (pre_line || post_line)) {
    writer->indent = ((rule->indent >= 0 || writer->indent >= -rule->indent)
                        ? writer->indent + rule->indent
                        : 0);
  }

  // If this is the first comma, bump the increment for the following object
  if (sep == SEP_END_O && !writer->context.comma_indented) {
    ++writer->indent;
    writer->context.comma_indented = true;
  }

  // Write newline or space before separator if necessary
  if (pre_line) {
    TRY(st, write_newline(writer));
  } else if (rule->pre_space_after & (1U << writer->last_sep)) {
    TRY(st, esink(" ", 1, writer));
  }

  // Write actual separator string
  if (rule->sep) {
    TRY(st, esink(&rule->sep, 1, writer));
  }

  // Write newline after separator if necessary
  if (post_line) {
    TRY(st, write_newline(writer));
    if (rule->post_line_after != ~(SepMask)0U) {
      writer->last_sep = SEP_NEWLINE;
    }
  }

  // Reset context and write a blank line after ends of subjects
  if (sep == SEP_END_S) {
    writer->indent                 = writer->context.graph.type ? 1 : 0;
    writer->context.predicates     = false;
    writer->context.comma_indented = false;
    TRY(st, esink("\n", 1, writer));
  }

  writer->last_sep = sep;
  return st;
}

static void
free_anon_stack(SerdWriter* writer)
{
  while (!serd_stack_is_empty(&writer->anon_stack)) {
    pop_context(writer);
  }
}

static SerdStatus
reset_context(SerdWriter* writer, const unsigned flags)
{
  free_anon_stack(writer);

  if (flags & RESET_GRAPH) {
    writer->context.graph.type = SERD_NOTHING;
  }

  if (flags & RESET_INDENT) {
    writer->indent = 0;
  }

  writer->context.type           = CTX_NAMED;
  writer->context.subject.type   = SERD_NOTHING;
  writer->context.predicate.type = SERD_NOTHING;
  writer->context.predicates     = false;
  writer->context.comma_indented = false;
  return SERD_SUCCESS;
}

SERD_NODISCARD static SerdStatus
write_literal(SerdWriter*        writer,
              const SerdNode*    node,
              const SerdNode*    datatype,
              const SerdNode*    lang,
              SerdStatementFlags flags)
{
  SerdStatus st = SERD_SUCCESS;

  if (supports_abbrev(writer) && datatype && datatype->buf) {
    const char* type_uri = (const char*)datatype->buf;
    if (!strncmp(type_uri, NS_XSD, sizeof(NS_XSD) - 1) &&
        (!strcmp(type_uri + sizeof(NS_XSD) - 1, "boolean") ||
         !strcmp(type_uri + sizeof(NS_XSD) - 1, "integer"))) {
      return esink(node->buf, node->n_bytes, writer);
    }

    if (!strncmp(type_uri, NS_XSD, sizeof(NS_XSD) - 1) &&
        !strcmp(type_uri + sizeof(NS_XSD) - 1, "decimal") &&
        strchr((const char*)node->buf, '.') &&
        node->buf[node->n_bytes - 1] != '.') {
      /* xsd:decimal literals without trailing digits, e.g. "5.", can
         not be written bare in Turtle.  We could add a 0 which is
         prettier, but changes the text and breaks round tripping.
      */
      return esink(node->buf, node->n_bytes, writer);
    }
  }

  if (supports_abbrev(writer) &&
      (node->flags & (SERD_HAS_NEWLINE | SERD_HAS_QUOTE))) {
    TRY(st, esink("\"\"\"", 3, writer));
    TRY(st, write_text(writer, WRITE_LONG_STRING, node->buf, node->n_bytes));
    TRY(st, esink("\"\"\"", 3, writer));
  } else {
    TRY(st, esink("\"", 1, writer));
    TRY(st, write_text(writer, WRITE_STRING, node->buf, node->n_bytes));
    TRY(st, esink("\"", 1, writer));
  }
  if (lang && lang->buf) {
    TRY(st, esink("@", 1, writer));
    TRY(st, esink(lang->buf, lang->n_bytes, writer));
  } else if (datatype && datatype->buf) {
    TRY(st, esink("^^", 2, writer));
    return write_node(writer, datatype, NULL, NULL, FIELD_NONE, flags);
  }

  return st;
}

// Return true iff `buf` is a valid prefixed name prefix or suffix
static bool
is_name(const uint8_t* buf, const size_t len)
{
  // TODO: This is more strict than it should be
  for (size_t i = 0; i < len; ++i) {
    if (!(is_alpha(buf[i]) || is_digit(buf[i]))) {
      return false;
    }
  }

  return true;
}

SERD_NODISCARD static SerdStatus
write_uri_node(SerdWriter* const writer,
               const SerdNode*   node,
               const Field       field)
{
  SerdStatus st     = SERD_SUCCESS;
  SerdNode   prefix = SERD_NODE_NULL;
  SerdChunk  suffix = {NULL, 0U};

  const bool has_scheme = serd_uri_string_has_scheme(node->buf);
  if (supports_abbrev(writer)) {
    if (field == FIELD_PREDICATE &&
        !strcmp((const char*)node->buf, NS_RDF "type")) {
      return esink("a", 1, writer);
    }

    if (!strcmp((const char*)node->buf, NS_RDF "nil")) {
      return esink("()", 2, writer);
    }

    if (has_scheme && (writer->style & SERD_STYLE_CURIED) &&
        serd_env_qualify(writer->env, node, &prefix, &suffix) &&
        is_name(prefix.buf, prefix.n_bytes) &&
        is_name(suffix.buf, suffix.len)) {
      TRY(st, write_uri_from_node(writer, &prefix));
      TRY(st, esink(":", 1, writer));
      return ewrite_uri(writer, suffix.buf, suffix.len);
    }
  }

  if (!has_scheme && !supports_uriref(writer) &&
      !serd_env_get_base_uri(writer->env, NULL)->buf) {
    return w_err(writer,
                 SERD_ERR_BAD_ARG,
                 "syntax does not support URI reference <%s>\n",
                 node->buf);
  }

  TRY(st, esink("<", 1, writer));

  if (writer->style & SERD_STYLE_RESOLVED) {
    SerdURI in_base_uri;
    SerdURI uri;
    SerdURI abs_uri;
    serd_env_get_base_uri(writer->env, &in_base_uri);
    serd_uri_parse(node->buf, &uri);
    serd_uri_resolve(&uri, &in_base_uri, &abs_uri);
    bool           rooted = uri_is_under(&writer->base_uri, &writer->root_uri);
    SerdURI*       root   = rooted ? &writer->root_uri : &writer->base_uri;
    UriSinkContext ctx    = {writer, SERD_SUCCESS};
    if (!uri_is_under(&abs_uri, root) || writer->syntax == SERD_NTRIPLES ||
        writer->syntax == SERD_NQUADS) {
      serd_uri_serialise(&abs_uri, uri_sink, &ctx);
    } else {
      serd_uri_serialise_relative(
        &uri, &writer->base_uri, root, uri_sink, &ctx);
    }
  } else {
    TRY(st, write_uri_from_node(writer, node));
  }

  return esink(">", 1, writer);
}

SERD_NODISCARD static SerdStatus
write_curie(SerdWriter* const writer, const SerdNode* const node)
{
  SerdChunk  prefix = {NULL, 0};
  SerdChunk  suffix = {NULL, 0};
  SerdStatus st     = SERD_SUCCESS;

  // In fast-and-loose Turtle/TriG mode CURIEs are simply passed through
  const bool fast =
    !(writer->style & (SERD_STYLE_CURIED | SERD_STYLE_RESOLVED));

  if (!supports_abbrev(writer) || !fast) {
    if ((st = serd_env_expand(writer->env, node, &prefix, &suffix))) {
      return w_err(writer, st, "undefined namespace prefix '%s'\n", node->buf);
    }
  }

  if (!supports_abbrev(writer)) {
    TRY(st, esink("<", 1, writer));
    TRY(st, ewrite_uri(writer, prefix.buf, prefix.len));
    TRY(st, ewrite_uri(writer, suffix.buf, suffix.len));
    TRY(st, esink(">", 1, writer));
  } else {
    TRY(st, write_lname(writer, node->buf, node->n_bytes));
  }

  return st;
}

SERD_NODISCARD static SerdStatus
write_blank(SerdWriter* const        writer,
            const SerdNode*          node,
            const Field              field,
            const SerdStatementFlags flags)
{
  SerdStatus st = SERD_SUCCESS;

  if (supports_abbrev(writer)) {
    if ((field == FIELD_SUBJECT && (flags & SERD_ANON_S_BEGIN)) ||
        (field == FIELD_OBJECT && (flags & SERD_ANON_O_BEGIN))) {
      return write_sep(writer, SEP_ANON_BEGIN);
    }

    if ((field == FIELD_SUBJECT && (flags & SERD_LIST_S_BEGIN)) ||
        (field == FIELD_OBJECT && (flags & SERD_LIST_O_BEGIN))) {
      return write_sep(writer, SEP_LIST_BEGIN);
    }

    if ((field == FIELD_SUBJECT && (flags & SERD_EMPTY_S)) ||
        (field == FIELD_OBJECT && (flags & SERD_EMPTY_O))) {
      return esink("[]", 2, writer);
    }
  }

  TRY(st, esink("_:", 2, writer));
  if (writer->bprefix && !strncmp((const char*)node->buf,
                                  (const char*)writer->bprefix,
                                  writer->bprefix_len)) {
    TRY(st,
        esink(node->buf + writer->bprefix_len,
              node->n_bytes - writer->bprefix_len,
              writer));
  } else {
    TRY(st, esink(node->buf, node->n_bytes, writer));
  }

  return st;
}

SERD_NODISCARD static SerdStatus
write_node(SerdWriter*        writer,
           const SerdNode*    node,
           const SerdNode*    datatype,
           const SerdNode*    lang,
           Field              field,
           SerdStatementFlags flags)
{
  SerdStatus st = SERD_SUCCESS;

  switch (node->type) {
  case SERD_NOTHING:
    break;
  case SERD_LITERAL:
    st = write_literal(writer, node, datatype, lang, flags);
    break;
  case SERD_URI:
    st = write_uri_node(writer, node, field);
    break;
  case SERD_CURIE:
    st = write_curie(writer, node);
    break;
  case SERD_BLANK:
    st = write_blank(writer, node, field, flags);
    break;
  }

  if (node->type != SERD_BLANK) {
    writer->last_sep = SEP_NODE;
  }

  return st;
}

static bool
is_resource(const SerdNode* node)
{
  return node && node->buf && node->type > SERD_LITERAL;
}

SERD_NODISCARD static SerdStatus
write_pred(SerdWriter* writer, SerdStatementFlags flags, const SerdNode* pred)
{
  SerdStatus st = SERD_SUCCESS;

  TRY(st, write_node(writer, pred, NULL, NULL, FIELD_PREDICATE, flags));
  TRY(st, write_sep(writer, SEP_P_O));

  copy_node(&writer->context.predicate, pred);
  writer->context.predicates     = true;
  writer->context.comma_indented = false;
  return st;
}

SERD_NODISCARD static SerdStatus
write_list_next(SerdWriter*        writer,
                SerdStatementFlags flags,
                const SerdNode*    predicate,
                const SerdNode*    object,
                const SerdNode*    datatype,
                const SerdNode*    lang)
{
  SerdStatus st = SERD_SUCCESS;

  if (!strcmp((const char*)object->buf, NS_RDF "nil")) {
    TRY(st, write_sep(writer, SEP_LIST_END));
    return SERD_FAILURE;
  }

  if (!strcmp((const char*)predicate->buf, NS_RDF "first")) {
    TRY(st, write_node(writer, object, datatype, lang, FIELD_OBJECT, flags));
  } else {
    TRY(st, write_sep(writer, SEP_LIST_SEP));
  }

  return st;
}

SERD_NODISCARD static SerdStatus
terminate_context(SerdWriter* writer)
{
  SerdStatus st = SERD_SUCCESS;

  if (writer->context.subject.type) {
    TRY(st, write_sep(writer, SEP_END_S));
  }

  if (writer->context.graph.type) {
    TRY(st, write_sep(writer, SEP_GRAPH_END));
  }

  return st;
}

SerdStatus
serd_writer_write_statement(SerdWriter*        writer,
                            SerdStatementFlags flags,
                            const SerdNode*    graph,
                            const SerdNode*    subject,
                            const SerdNode*    predicate,
                            const SerdNode*    object,
                            const SerdNode*    datatype,
                            const SerdNode*    lang)
{
  SerdStatus st = SERD_SUCCESS;

  if (!is_resource(subject) || !is_resource(predicate) || !object ||
      !object->buf) {
    return SERD_ERR_BAD_ARG;
  }

  // Simple case: write a line of NTriples or NQuads
  if (writer->syntax == SERD_NTRIPLES || writer->syntax == SERD_NQUADS) {
    TRY(st, write_node(writer, subject, NULL, NULL, FIELD_SUBJECT, flags));
    TRY(st, esink(" ", 1, writer));
    TRY(st, write_node(writer, predicate, NULL, NULL, FIELD_PREDICATE, flags));
    TRY(st, esink(" ", 1, writer));
    TRY(st, write_node(writer, object, datatype, lang, FIELD_OBJECT, flags));
    if (writer->syntax == SERD_NQUADS && graph) {
      TRY(st, esink(" ", 1, writer));
      TRY(st, write_node(writer, graph, datatype, lang, FIELD_GRAPH, flags));
    }
    TRY(st, esink(" .\n", 3, writer));
    return SERD_SUCCESS;
  }

  // Separate graphs if necessary
  if ((graph && !serd_node_equals(graph, &writer->context.graph)) ||
      (!graph && writer->context.graph.type)) {
    TRY(st, terminate_context(writer));
    reset_context(writer, RESET_GRAPH | RESET_INDENT);
    if (graph) {
      TRY(st, write_newline(writer));
      TRY(st, write_node(writer, graph, datatype, lang, FIELD_GRAPH, flags));
      TRY(st, write_sep(writer, SEP_GRAPH_BEGIN));
      copy_node(&writer->context.graph, graph);
    }
  }

  if ((flags & SERD_LIST_CONT)) {
    // Continue a list
    if (!strcmp((const char*)predicate->buf, NS_RDF "first") &&
        !strcmp((const char*)object->buf, NS_RDF "nil")) {
      return esink("()", 2, writer);
    }

    TRY_FAILING(
      st, write_list_next(writer, flags, predicate, object, datatype, lang));

    if (st == SERD_FAILURE) { // Reached end of list
      pop_context(writer);
      return SERD_SUCCESS;
    }

  } else if (serd_node_equals(subject, &writer->context.subject)) {
    if (serd_node_equals(predicate, &writer->context.predicate)) {
      // Elide S P (write O)

      const Sep  last      = writer->last_sep;
      const bool anon_o    = flags & SERD_ANON_O_BEGIN;
      const bool list_o    = flags & SERD_LIST_O_BEGIN;
      const bool open_o    = anon_o || list_o;
      const bool after_end = (last == SEP_ANON_END) || (last == SEP_LIST_END);

      TRY(st,
          write_sep(writer,
                    after_end ? (open_o ? SEP_JOIN_O_AA : SEP_JOIN_O_AN)
                              : (open_o ? SEP_JOIN_O_NA : SEP_END_O)));

    } else {
      // Elide S (write P and O)

      if (writer->context.comma_indented) {
        --writer->indent;
        writer->context.comma_indented = false;
      }

      const bool first = !writer->context.predicate.type;
      TRY(st, write_sep(writer, first ? SEP_S_P : SEP_END_P));
      TRY(st, write_pred(writer, flags, predicate));
    }

    TRY(st, write_node(writer, object, datatype, lang, FIELD_OBJECT, flags));

  } else {
    // No abbreviation
    if (serd_stack_is_empty(&writer->anon_stack)) {
      if (writer->context.subject.type) {
        TRY(st, write_sep(writer, SEP_END_S));
      }

      if (writer->last_sep == SEP_END_S || writer->last_sep == SEP_END_DIRECT) {
        TRY(st, write_newline(writer));
      }

      TRY(st, write_node(writer, subject, NULL, NULL, FIELD_SUBJECT, flags));
      if ((flags & (SERD_ANON_S_BEGIN | SERD_LIST_S_BEGIN))) {
        TRY(st, write_sep(writer, SEP_ANON_S_P));
      } else {
        TRY(st, write_sep(writer, SEP_S_P));
      }

    } else {
      TRY(st, write_sep(writer, SEP_ANON_S_P));
    }

    reset_context(writer, 0U);
    copy_node(&writer->context.subject, subject);

    if (!(flags & SERD_LIST_S_BEGIN)) {
      TRY(st, write_pred(writer, flags, predicate));
    }

    TRY(st, write_node(writer, object, datatype, lang, FIELD_OBJECT, flags));
  }

  if (flags & (SERD_ANON_S_BEGIN | SERD_LIST_S_BEGIN)) {
    // Push context for anonymous or list subject
    const bool is_list = (flags & SERD_LIST_S_BEGIN);
    push_context(writer,
                 is_list ? CTX_LIST : CTX_BLANK,
                 serd_node_copy(graph),
                 serd_node_copy(subject),
                 is_list ? SERD_NODE_NULL : serd_node_copy(predicate));
  }

  if (flags & (SERD_ANON_O_BEGIN | SERD_LIST_O_BEGIN)) {
    // Push context for anonymous or list object if necessary
    push_context(writer,
                 (flags & SERD_LIST_O_BEGIN) ? CTX_LIST : CTX_BLANK,
                 serd_node_copy(graph),
                 serd_node_copy(object),
                 SERD_NODE_NULL);
  }

  return st;
}

SerdStatus
serd_writer_end_anon(SerdWriter* writer, const SerdNode* node)
{
  SerdStatus st = SERD_SUCCESS;

  if (writer->syntax == SERD_NTRIPLES || writer->syntax == SERD_NQUADS) {
    return SERD_SUCCESS;
  }

  if (serd_stack_is_empty(&writer->anon_stack)) {
    return w_err(
      writer, SERD_ERR_UNKNOWN, "unexpected end of anonymous node\n");
  }

  // Write the end separator ']' and pop the context
  TRY(st, write_sep(writer, SEP_ANON_END));
  pop_context(writer);

  if (serd_node_equals(node, &writer->context.subject)) {
    // Now-finished anonymous node is the new subject with no other context
    writer->context.predicate.type = SERD_NOTHING;
  }

  return st;
}

SerdStatus
serd_writer_finish(SerdWriter* writer)
{
  const SerdStatus st0 = terminate_context(writer);
  const SerdStatus st1 = serd_byte_sink_flush(&writer->byte_sink);
  free_anon_stack(writer);
  reset_context(writer, RESET_GRAPH | RESET_INDENT);
  return st0 ? st0 : st1;
}

SerdWriter*
serd_writer_new(SerdSyntax     syntax,
                SerdStyle      style,
                SerdEnv*       env,
                const SerdURI* base_uri,
                SerdSink       ssink,
                void*          stream)
{
  const WriteContext context = WRITE_CONTEXT_NULL;
  SerdWriter*        writer  = (SerdWriter*)calloc(1, sizeof(SerdWriter));

  writer->syntax     = syntax;
  writer->style      = style;
  writer->env        = env;
  writer->root_node  = SERD_NODE_NULL;
  writer->root_uri   = SERD_URI_NULL;
  writer->base_uri   = base_uri ? *base_uri : SERD_URI_NULL;
  writer->anon_stack = serd_stack_new(SERD_PAGE_SIZE);
  writer->context    = context;
  writer->byte_sink  = serd_byte_sink_new(
    ssink, stream, (style & SERD_STYLE_BULK) ? SERD_PAGE_SIZE : 1);

  return writer;
}

void
serd_writer_set_error_sink(SerdWriter*   writer,
                           SerdErrorSink error_sink,
                           void*         error_handle)
{
  writer->error_sink   = error_sink;
  writer->error_handle = error_handle;
}

void
serd_writer_chop_blank_prefix(SerdWriter* writer, const uint8_t* prefix)
{
  free(writer->bprefix);
  writer->bprefix_len = 0;
  writer->bprefix     = NULL;

  const size_t prefix_len = prefix ? strlen((const char*)prefix) : 0;
  if (prefix_len) {
    writer->bprefix_len = prefix_len;
    writer->bprefix     = (uint8_t*)malloc(writer->bprefix_len + 1);
    memcpy(writer->bprefix, prefix, writer->bprefix_len + 1);
  }
}

SerdStatus
serd_writer_set_base_uri(SerdWriter* writer, const SerdNode* uri)
{
  SerdStatus st = SERD_SUCCESS;

  TRY(st, serd_env_set_base_uri(writer->env, uri));

  serd_env_get_base_uri(writer->env, &writer->base_uri);

  if (uri && (writer->syntax == SERD_TURTLE || writer->syntax == SERD_TRIG)) {
    TRY(st, terminate_context(writer));
    TRY(st, esink("@base <", 7, writer));
    TRY(st, esink(uri->buf, uri->n_bytes, writer));
    TRY(st, esink(">", 1, writer));
    writer->last_sep = SEP_NODE;
    TRY(st, write_sep(writer, SEP_END_DIRECT));
  }

  return reset_context(writer, RESET_GRAPH | RESET_INDENT);
}

SerdStatus
serd_writer_set_root_uri(SerdWriter* writer, const SerdNode* uri)
{
  serd_node_free(&writer->root_node);

  if (uri && uri->buf) {
    writer->root_node = serd_node_copy(uri);
    serd_uri_parse(uri->buf, &writer->root_uri);
  } else {
    writer->root_node = SERD_NODE_NULL;
    writer->root_uri  = SERD_URI_NULL;
  }

  return SERD_SUCCESS;
}

SerdStatus
serd_writer_set_prefix(SerdWriter*     writer,
                       const SerdNode* name,
                       const SerdNode* uri)
{
  SerdStatus st = SERD_SUCCESS;

  TRY(st, serd_env_set_prefix(writer->env, name, uri));

  if (writer->syntax == SERD_TURTLE || writer->syntax == SERD_TRIG) {
    TRY(st, terminate_context(writer));
    TRY(st, esink("@prefix ", 8, writer));
    TRY(st, esink(name->buf, name->n_bytes, writer));
    TRY(st, esink(": <", 3, writer));
    TRY(st, ewrite_uri(writer, uri->buf, uri->n_bytes));
    TRY(st, esink(">", 1, writer));
    writer->last_sep = SEP_NODE;
    TRY(st, write_sep(writer, SEP_END_DIRECT));
  }

  return reset_context(writer, RESET_GRAPH | RESET_INDENT);
}

void
serd_writer_free(SerdWriter* writer)
{
  if (!writer) {
    return;
  }

  serd_writer_finish(writer);
  free_context(&writer->context);
  free_anon_stack(writer);
  serd_stack_free(&writer->anon_stack);
  free(writer->bprefix);
  serd_byte_sink_free(&writer->byte_sink);
  serd_node_free(&writer->root_node);
  free(writer);
}

SerdEnv*
serd_writer_get_env(SerdWriter* writer)
{
  return writer->env;
}

size_t
serd_file_sink(const void* buf, size_t len, void* stream)
{
  return fwrite(buf, 1, len, (FILE*)stream);
}

size_t
serd_chunk_sink(const void* buf, size_t len, void* stream)
{
  SerdChunk* chunk = (SerdChunk*)stream;
  chunk->buf       = (uint8_t*)realloc((uint8_t*)chunk->buf, chunk->len + len);
  memcpy((uint8_t*)chunk->buf + chunk->len, buf, len);
  chunk->len += len;
  return len;
}

uint8_t*
serd_chunk_sink_finish(SerdChunk* stream)
{
  serd_chunk_sink("", 1, stream);
  return (uint8_t*)stream->buf;
}
