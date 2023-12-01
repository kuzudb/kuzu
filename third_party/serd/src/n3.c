// Copyright 2011-2023 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#include "reader.h"
#include "serd_internal.h"
#include "stack.h"
#include "string_utils.h"
#include "try.h"
#include "uri_utils.h"

#include "serd.h"

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#if defined(__clang__) && __clang_major__ >= 10
#  define SERD_FALLTHROUGH __attribute__((fallthrough))
_Pragma("clang diagnostic push")
_Pragma("clang diagnostic ignored \"-Wmissing-declarations\"")
#elif defined(__GNUC__) && __GNUC__ >= 7
#  define SERD_FALLTHROUGH __attribute__((fallthrough))
#else
#  define SERD_FALLTHROUGH
#endif

static bool
fancy_syntax(const SerdReader* const reader)
{
  return reader->syntax == SERD_TURTLE || reader->syntax == SERD_TRIG;
}

static SerdStatus
read_collection(SerdReader* reader, ReadContext ctx, Ref* dest);

static SerdStatus
read_predicateObjectList(SerdReader* reader, ReadContext ctx, bool* ate_dot);

static uint8_t
read_HEX(SerdReader* const reader)
{
  const int c = peek_byte(reader);
  if (is_xdigit(c)) {
    return (uint8_t)eat_byte_safe(reader, c);
  }

  r_err(reader, SERD_ERR_BAD_SYNTAX, "invalid hexadecimal digit '%c'\n", c);
  return 0;
}

// Read UCHAR escape, initial \ is already eaten by caller
static SerdStatus
read_UCHAR(SerdReader* const reader, const Ref dest, uint32_t* const char_code)
{
  const int b      = peek_byte(reader);
  unsigned  length = 0;
  switch (b) {
  case 'U':
    length = 8;
    break;
  case 'u':
    length = 4;
    break;
  default:
    return SERD_ERR_BAD_SYNTAX;
  }

  skip_byte(reader, b);

  // Read character code point in hex
  uint8_t  buf[9] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
  uint32_t code   = 0U;
  for (unsigned i = 0U; i < length; ++i) {
    if (!(buf[i] = read_HEX(reader))) {
      return SERD_ERR_BAD_SYNTAX;
    }

    code = (code << (i ? 4U : 0U)) | hex_digit_value(buf[i]);
  }

  // Determine the encoded size from the code point
  unsigned size = 0;
  if (code < 0x00000080) {
    size = 1;
  } else if (code < 0x00000800) {
    size = 2;
  } else if (code < 0x00010000) {
    size = 3;
  } else if (code < 0x00110000) {
    size = 4;
  } else {
    r_err(reader,
          SERD_ERR_BAD_SYNTAX,
          "unicode character 0x%X out of range\n",
          code);
    push_bytes(reader, dest, replacement_char, 3);
    *char_code = 0xFFFD;
    return SERD_SUCCESS;
  }

  // Build output in buf
  // (Note # of bytes = # of leading 1 bits in first byte)
  uint32_t c = code;
  switch (size) {
  case 4:
    buf[3] = (uint8_t)(0x80U | (c & 0x3FU));
    c >>= 6;
    c |= (16 << 12); // set bit 4
    SERD_FALLTHROUGH;
  case 3:
    buf[2] = (uint8_t)(0x80U | (c & 0x3FU));
    c >>= 6;
    c |= (32 << 6); // set bit 5
    SERD_FALLTHROUGH;
  case 2:
    buf[1] = (uint8_t)(0x80U | (c & 0x3FU));
    c >>= 6;
    c |= 0xC0; // set bits 6 and 7
    SERD_FALLTHROUGH;
  case 1:
    buf[0] = (uint8_t)c;
    SERD_FALLTHROUGH;
  default:
    break;
  }

  push_bytes(reader, dest, buf, size);
  *char_code = code;
  return SERD_SUCCESS;
}

// Read ECHAR escape, initial \ is already eaten by caller
static SerdStatus
read_ECHAR(SerdReader* const reader, const Ref dest, SerdNodeFlags* const flags)
{
  SerdStatus st = SERD_SUCCESS;
  const int  c  = peek_byte(reader);
  switch (c) {
  case 't':
    return (st = skip_byte(reader, 't')) ? st : push_byte(reader, dest, '\t');
  case 'b':
    return (st = skip_byte(reader, 'b')) ? st : push_byte(reader, dest, '\b');
  case 'n':
    *flags |= SERD_HAS_NEWLINE;
    return (st = skip_byte(reader, 'n')) ? st : push_byte(reader, dest, '\n');
  case 'r':
    *flags |= SERD_HAS_NEWLINE;
    return (st = skip_byte(reader, 'r')) ? st : push_byte(reader, dest, '\r');
  case 'f':
    return (st = skip_byte(reader, 'f')) ? st : push_byte(reader, dest, '\f');
  case '\\':
  case '"':
  case '\'':
    return push_byte(reader, dest, eat_byte_safe(reader, c));
  default:
    return SERD_ERR_BAD_SYNTAX;
  }
}

static SerdStatus
bad_char(SerdReader* const reader, const char* const fmt, const uint8_t c)
{
  // Skip bytes until the next start byte
  for (int b = peek_byte(reader); b != EOF && ((uint8_t)b & 0x80);) {
    skip_byte(reader, b);
    b = peek_byte(reader);
  }

  r_err(reader, SERD_ERR_BAD_SYNTAX, fmt, c);
  return reader->strict ? SERD_ERR_BAD_SYNTAX : SERD_FAILURE;
}

static SerdStatus
read_utf8_bytes(SerdReader* const reader,
                uint8_t           bytes[4],
                uint32_t* const   size,
                const uint8_t     c)
{
  *size = utf8_num_bytes(c);
  if (*size <= 1 || *size > 4) {
    return bad_char(reader, "invalid UTF-8 start 0x%X\n", c);
  }

  bytes[0] = c;
  for (unsigned i = 1; i < *size; ++i) {
    const int b = peek_byte(reader);
    if (b == EOF || ((uint8_t)b & 0x80) == 0) {
      return bad_char(reader, "invalid UTF-8 continuation 0x%X\n", (uint8_t)b);
    }

    bytes[i] = (uint8_t)eat_byte_safe(reader, b);
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_utf8_character(SerdReader* const reader, const Ref dest, const uint8_t c)
{
  uint32_t   size     = 0;
  uint8_t    bytes[4] = {0, 0, 0, 0};
  SerdStatus st       = read_utf8_bytes(reader, bytes, &size, c);
  if (st) {
    push_bytes(reader, dest, replacement_char, 3);
  } else {
    push_bytes(reader, dest, bytes, size);
  }

  return st;
}

static SerdStatus
read_utf8_code(SerdReader* const reader,
               const Ref         dest,
               uint32_t* const   code,
               const uint8_t     c)
{
  uint32_t   size     = 0;
  uint8_t    bytes[4] = {0, 0, 0, 0};
  SerdStatus st       = read_utf8_bytes(reader, bytes, &size, c);
  if (st) {
    push_bytes(reader, dest, replacement_char, 3);
    return st;
  }

  push_bytes(reader, dest, bytes, size);
  *code = parse_counted_utf8_char(bytes, size);
  return st;
}

// Read one character (possibly multi-byte)
// The first byte, c, has already been eaten by caller
static SerdStatus
read_character(SerdReader* const    reader,
               const Ref            dest,
               SerdNodeFlags* const flags,
               const uint8_t        c)
{
  if (!(c & 0x80)) {
    switch (c) {
    case 0xA:
    case 0xD:
      *flags |= SERD_HAS_NEWLINE;
      break;
    case '"':
    case '\'':
      *flags |= SERD_HAS_QUOTE;
      break;
    default:
      break;
    }
    return push_byte(reader, dest, c);
  }

  return read_utf8_character(reader, dest, c);
}

// [10] comment ::= '#' ( [^#xA #xD] )*
static void
read_comment(SerdReader* const reader)
{
  skip_byte(reader, '#');

  int c = 0;
  while (((c = peek_byte(reader)) != 0xA) && c != 0xD && c != EOF && c) {
    skip_byte(reader, c);
  }
}

// [24] ws ::= #x9 | #xA | #xD | #x20 | comment
static bool
read_ws(SerdReader* const reader)
{
  const int c = peek_byte(reader);
  switch (c) {
  case 0x9:
  case 0xA:
  case 0xD:
  case 0x20:
    skip_byte(reader, c);
    return true;
  case '#':
    read_comment(reader);
    return true;
  default:
    return false;
  }
}

static bool
read_ws_star(SerdReader* const reader)
{
  while (read_ws(reader)) {
  }

  return true;
}

static bool
peek_delim(SerdReader* const reader, const uint8_t delim)
{
  read_ws_star(reader);
  return peek_byte(reader) == delim;
}

static bool
eat_delim(SerdReader* const reader, const uint8_t delim)
{
  if (peek_delim(reader, delim)) {
    skip_byte(reader, delim);
    return read_ws_star(reader);
  }

  return false;
}

static SerdStatus
read_string_escape(SerdReader* const    reader,
                   const Ref            ref,
                   SerdNodeFlags* const flags)
{
  SerdStatus st   = SERD_SUCCESS;
  uint32_t   code = 0;
  if ((st = read_ECHAR(reader, ref, flags)) &&
      (st = read_UCHAR(reader, ref, &code))) {
    return r_err(reader, st, "invalid escape '\\%c'\n", peek_byte(reader));
  }

  return st;
}

// STRING_LITERAL_LONG_QUOTE and STRING_LITERAL_LONG_SINGLE_QUOTE
// Initial triple quotes are already eaten by caller
static SerdStatus
read_STRING_LITERAL_LONG(SerdReader* const    reader,
                         const Ref            ref,
                         SerdNodeFlags* const flags,
                         const uint8_t        q)
{
  SerdStatus st = SERD_SUCCESS;

  while (!(st && reader->strict)) {
    const int c = peek_byte(reader);
    if (c == '\\') {
      skip_byte(reader, c);
      st = read_string_escape(reader, ref, flags);
    } else if (c == q) {
      skip_byte(reader, q);
      const int q2 = eat_byte_safe(reader, peek_byte(reader));
      const int q3 = peek_byte(reader);
      if (q2 == q && q3 == q) { // End of string
        skip_byte(reader, q3);
        break;
      }

      if (q2 == '\\') {
        push_byte(reader, ref, c);
        st = read_string_escape(reader, ref, flags);
      } else {
        *flags |= SERD_HAS_QUOTE;
        push_byte(reader, ref, c);
        st = read_character(reader, ref, flags, (uint8_t)q2);
      }
    } else if (c == EOF) {
      st = r_err(reader, SERD_ERR_BAD_SYNTAX, "end of file in long string\n");
    } else {
      st =
        read_character(reader, ref, flags, (uint8_t)eat_byte_safe(reader, c));
    }
  }

  return (st && reader->strict) ? st : SERD_SUCCESS;
}

// STRING_LITERAL_QUOTE and STRING_LITERAL_SINGLE_QUOTE
// Initial quote is already eaten by caller
static SerdStatus
read_STRING_LITERAL(SerdReader* const    reader,
                    const Ref            ref,
                    SerdNodeFlags* const flags,
                    const uint8_t        q)
{
  SerdStatus st = SERD_SUCCESS;

  while (!st || !reader->strict) {
    const int c = peek_byte(reader);
    switch (c) {
    case EOF:
      return r_err(
        reader, SERD_ERR_BAD_SYNTAX, "end of file in short string\n");
    case '\n':
    case '\r':
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "line end in short string\n");
    case '\\':
      skip_byte(reader, c);
      TRY(st, read_string_escape(reader, ref, flags));
      break;
    default:
      if (c == q) {
        return skip_byte(reader, c);
      }

      st =
        read_character(reader, ref, flags, (uint8_t)eat_byte_safe(reader, c));
    }
  }

  return eat_byte_check(reader, q) ? SERD_SUCCESS : SERD_ERR_BAD_SYNTAX;
}

static SerdStatus
read_String(SerdReader* const    reader,
            const Ref            node,
            SerdNodeFlags* const flags)
{
  const int q1 = eat_byte_safe(reader, peek_byte(reader));
  const int q2 = peek_byte(reader);
  if (q2 == EOF) {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "unexpected end of file\n");
  }

  if (q2 != q1) { // Short string (not triple quoted)
    return read_STRING_LITERAL(reader, node, flags, (uint8_t)q1);
  }

  skip_byte(reader, q2);
  const int q3 = peek_byte(reader);
  if (q3 == EOF) {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "unexpected end of file\n");
  }

  if (q3 != q1) { // Empty short string ("" or '')
    return SERD_SUCCESS;
  }

  if (!fancy_syntax(reader)) {
    return r_err(
      reader, SERD_ERR_BAD_SYNTAX, "syntax does not support long literals\n");
  }

  skip_byte(reader, q3);
  return read_STRING_LITERAL_LONG(reader, node, flags, (uint8_t)q1);
}

static bool
is_PN_CHARS_BASE(const uint32_t c)
{
  return ((c >= 0x00C0 && c <= 0x00D6) || (c >= 0x00D8 && c <= 0x00F6) ||
          (c >= 0x00F8 && c <= 0x02FF) || (c >= 0x0370 && c <= 0x037D) ||
          (c >= 0x037F && c <= 0x1FFF) || (c >= 0x200C && c <= 0x200D) ||
          (c >= 0x2070 && c <= 0x218F) || (c >= 0x2C00 && c <= 0x2FEF) ||
          (c >= 0x3001 && c <= 0xD7FF) || (c >= 0xF900 && c <= 0xFDCF) ||
          (c >= 0xFDF0 && c <= 0xFFFD) || (c >= 0x10000 && c <= 0xEFFFF));
}

static SerdStatus
read_PN_CHARS_BASE(SerdReader* const reader, const Ref dest)
{
  uint32_t   code = 0;
  const int  c    = peek_byte(reader);
  SerdStatus st   = SERD_SUCCESS;

  if (is_alpha(c)) {
    return push_byte(reader, dest, eat_byte_safe(reader, c));
  }

  if (c == EOF || !(c & 0x80)) {
    return SERD_FAILURE;
  }

  skip_byte(reader, c);
  read_utf8_code(reader, dest, &code, (uint8_t)c);

  if (!is_PN_CHARS_BASE(code)) {
    r_err(
      reader, SERD_ERR_BAD_SYNTAX, "invalid character U+%04X in name\n", code);
    if (reader->strict) {
      return SERD_ERR_BAD_SYNTAX;
    }
  }

  return st;
}

static bool
is_PN_CHARS(const uint32_t c)
{
  return (is_PN_CHARS_BASE(c) || c == 0xB7 || (c >= 0x0300 && c <= 0x036F) ||
          (c >= 0x203F && c <= 0x2040));
}

static SerdStatus
read_PN_CHARS(SerdReader* const reader, const Ref dest)
{
  uint32_t   code = 0;
  const int  c    = peek_byte(reader);
  SerdStatus st   = SERD_SUCCESS;

  if (is_alpha(c) || is_digit(c) || c == '_' || c == '-') {
    return push_byte(reader, dest, eat_byte_safe(reader, c));
  }

  if (c == EOF || !(c & 0x80)) {
    return SERD_FAILURE;
  }

  skip_byte(reader, c);
  TRY(st, read_utf8_code(reader, dest, &code, (uint8_t)c));

  if (!is_PN_CHARS(code)) {
    return r_err(
      reader, SERD_ERR_BAD_SYNTAX, "invalid character U+%04X in name\n", code);
  }

  return st;
}

static SerdStatus
read_PERCENT(SerdReader* const reader, const Ref dest)
{
  push_byte(reader, dest, eat_byte_safe(reader, '%'));
  const uint8_t h1 = read_HEX(reader);
  const uint8_t h2 = read_HEX(reader);
  if (h1 && h2) {
    push_byte(reader, dest, h1);
    return push_byte(reader, dest, h2);
  }

  return SERD_ERR_BAD_SYNTAX;
}

static SerdStatus
read_PN_LOCAL_ESC(SerdReader* const reader, const Ref dest)
{
  skip_byte(reader, '\\');

  const int c = peek_byte(reader);
  switch (c) {
  case '!':
  case '#':
  case '$':
  case '%':
  case '&':
  case '\'':
  case '(':
  case ')':
  case '*':
  case '+':
  case ',':
  case '-':
  case '.':
  case '/':
  case ';':
  case '=':
  case '?':
  case '@':
  case '_':
  case '~':
    push_byte(reader, dest, eat_byte_safe(reader, c));
    break;
  default:
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "invalid escape\n");
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_PLX(SerdReader* const reader, const Ref dest)
{
  const int c = peek_byte(reader);
  switch (c) {
  case '%':
    return read_PERCENT(reader, dest);
  case '\\':
    return read_PN_LOCAL_ESC(reader, dest);
  default:
    return SERD_FAILURE;
  }
}

static SerdStatus
read_PN_LOCAL(SerdReader* const reader, const Ref dest, bool* const ate_dot)
{
  int        c                      = peek_byte(reader);
  SerdStatus st                     = SERD_SUCCESS;
  bool       trailing_unescaped_dot = false;
  switch (c) {
  case '0':
  case '1':
  case '2':
  case '3':
  case '4':
  case '5':
  case '6':
  case '7':
  case '8':
  case '9':
  case ':':
  case '_':
    push_byte(reader, dest, eat_byte_safe(reader, c));
    break;
  default:
    if ((st = read_PLX(reader, dest)) > SERD_FAILURE) {
      return r_err(reader, st, "bad escape\n");
    }

    if (st != SERD_SUCCESS && (st = read_PN_CHARS_BASE(reader, dest))) {
      return st;
    }
  }

  while ((c = peek_byte(reader))) { // Middle: (PN_CHARS | '.' | ':')*
    if (c == '.' || c == ':') {
      push_byte(reader, dest, eat_byte_safe(reader, c));
    } else if ((st = read_PLX(reader, dest)) > SERD_FAILURE) {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "bad escape\n");
    } else if (st != SERD_SUCCESS && (st = read_PN_CHARS(reader, dest))) {
      break;
    }
    trailing_unescaped_dot = (c == '.');
  }

  SerdNode* const n = deref(reader, dest);
  if (trailing_unescaped_dot) {
    // Ate trailing dot, pop it from stack/node and inform caller
    --n->n_bytes;
    serd_stack_pop(&reader->stack, 1);
    *ate_dot = true;
  }

  return (st > SERD_FAILURE) ? st : SERD_SUCCESS;
}

// Read the remainder of a PN_PREFIX after some initial characters
static SerdStatus
read_PN_PREFIX_tail(SerdReader* const reader, const Ref dest)
{
  int c = 0;
  while ((c = peek_byte(reader))) { // Middle: (PN_CHARS | '.')*
    if (c == '.') {
      push_byte(reader, dest, eat_byte_safe(reader, c));
    } else if (read_PN_CHARS(reader, dest)) {
      break;
    }
  }

  const SerdNode* const n = deref(reader, dest);
  if (n->buf[n->n_bytes - 1] == '.' && read_PN_CHARS(reader, dest)) {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "prefix ends with '.'\n");
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_PN_PREFIX(SerdReader* const reader, const Ref dest)
{
  const SerdStatus st = read_PN_CHARS_BASE(reader, dest);

  return st ? st : read_PN_PREFIX_tail(reader, dest);
}

static SerdStatus
read_LANGTAG(SerdReader* const reader, Ref* const dest)
{
  int c = peek_byte(reader);
  if (!is_alpha(c)) {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "unexpected '%c'\n", c);
  }

  *dest = push_node(reader, SERD_LITERAL, "", 0);

  SerdStatus st = SERD_SUCCESS;
  TRY(st, push_byte(reader, *dest, eat_byte_safe(reader, c)));
  while ((c = peek_byte(reader)) && is_alpha(c)) {
    TRY(st, push_byte(reader, *dest, eat_byte_safe(reader, c)));
  }

  while (peek_byte(reader) == '-') {
    TRY(st, push_byte(reader, *dest, eat_byte_safe(reader, '-')));
    while ((c = peek_byte(reader)) && (is_alpha(c) || is_digit(c))) {
      TRY(st, push_byte(reader, *dest, eat_byte_safe(reader, c)));
    }
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_IRIREF_scheme(SerdReader* const reader, const Ref dest)
{
  int c = peek_byte(reader);
  if (!is_alpha(c)) {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "bad IRI scheme start '%c'\n", c);
  }

  while ((c = peek_byte(reader)) != EOF) {
    if (c == '>') {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "missing IRI scheme\n");
    }

    if (!is_uri_scheme_char(c)) {
      return r_err(reader,
                   SERD_ERR_BAD_SYNTAX,
                   "bad IRI scheme char U+%04X (%c)\n",
                   (unsigned)c,
                   (char)c);
    }

    push_byte(reader, dest, eat_byte_safe(reader, c));
    if (c == ':') {
      return SERD_SUCCESS; // End of scheme
    }
  }

  return r_err(reader, SERD_ERR_BAD_SYNTAX, "unexpected end of file\n");
}

static SerdStatus
read_IRIREF(SerdReader* const reader, Ref* const dest)
{
  if (!eat_byte_check(reader, '<')) {
    return SERD_ERR_BAD_SYNTAX;
  }

  *dest = push_node(reader, SERD_URI, "", 0);

  if (!fancy_syntax(reader) && read_IRIREF_scheme(reader, *dest)) {
    *dest = pop_node(reader, *dest);
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "expected IRI scheme\n");
  }

  SerdStatus st   = SERD_SUCCESS;
  uint32_t   code = 0;
  while (st <= SERD_FAILURE) {
    const int c = eat_byte_safe(reader, peek_byte(reader));
    switch (c) {
    case '"':
    case '<':
      *dest = pop_node(reader, *dest);
      return r_err(
        reader, SERD_ERR_BAD_SYNTAX, "invalid IRI character '%c'\n", c);

    case '>':
      return SERD_SUCCESS;

    case '\\':
      if (read_UCHAR(reader, *dest, &code)) {
        *dest = pop_node(reader, *dest);
        return r_err(reader, SERD_ERR_BAD_SYNTAX, "invalid IRI escape\n");
      }

      switch (code) {
      case 0:
      case ' ':
      case '<':
      case '>':
        *dest = pop_node(reader, *dest);
        return r_err(reader,
                     SERD_ERR_BAD_SYNTAX,
                     "invalid escaped IRI character U+%04X\n",
                     code);
      default:
        break;
      }
      break;

    case '^':
    case '`':
    case '{':
    case '|':
    case '}':
      *dest = pop_node(reader, *dest);
      return r_err(
        reader, SERD_ERR_BAD_SYNTAX, "invalid IRI character '%c'\n", c);

    default:
      if (c <= 0x20) {
        st = r_err(reader,
                   SERD_ERR_BAD_SYNTAX,
                   "invalid IRI character (escape %%%02X)\n",
                   (unsigned)c);
        if (reader->strict) {
          break;
        }

        st = SERD_FAILURE;
        push_byte(reader, *dest, c);
      } else if (!(c & 0x80)) {
        push_byte(reader, *dest, c);
      } else if (read_utf8_character(reader, *dest, (uint8_t)c)) {
        if (reader->strict) {
          *dest = pop_node(reader, *dest);
          return SERD_ERR_BAD_SYNTAX;
        }
      }
    }
  }

  *dest = pop_node(reader, *dest);
  return st;
}

static SerdStatus
read_PrefixedName(SerdReader* const reader,
                  const Ref         dest,
                  const bool        read_prefix,
                  bool* const       ate_dot)
{
  SerdStatus st = SERD_SUCCESS;
  if (read_prefix) {
    TRY_FAILING(st, read_PN_PREFIX(reader, dest));
  }

  if (peek_byte(reader) != ':') {
    return SERD_FAILURE;
  }

  push_byte(reader, dest, eat_byte_safe(reader, ':'));

  TRY_FAILING(st, read_PN_LOCAL(reader, dest, ate_dot));
  return SERD_SUCCESS;
}

static SerdStatus
read_0_9(SerdReader* const reader, const Ref str, const bool at_least_one)
{
  unsigned   count = 0;
  SerdStatus st    = SERD_SUCCESS;
  for (int c = 0; is_digit((c = peek_byte(reader))); ++count) {
    TRY(st, push_byte(reader, str, eat_byte_safe(reader, c)));
  }

  if (at_least_one && count == 0) {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "expected digit\n");
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_number(SerdReader* const reader,
            Ref* const        dest,
            Ref* const        datatype,
            bool* const       ate_dot)
{
#define XSD_DECIMAL NS_XSD "decimal"
#define XSD_DOUBLE NS_XSD "double"
#define XSD_INTEGER NS_XSD "integer"

  *dest = push_node(reader, SERD_LITERAL, "", 0);

  SerdStatus st          = SERD_SUCCESS;
  int        c           = peek_byte(reader);
  bool       has_decimal = false;
  if (c == '-' || c == '+') {
    push_byte(reader, *dest, eat_byte_safe(reader, c));
  }
  if ((c = peek_byte(reader)) == '.') {
    has_decimal = true;
    // decimal case 2 (e.g. '.0' or '-.0' or '+.0')
    push_byte(reader, *dest, eat_byte_safe(reader, c));
    TRY(st, read_0_9(reader, *dest, true));
  } else {
    // all other cases ::= ( '-' | '+' ) [0-9]+ ( . )? ( [0-9]+ )? ...
    TRY(st, read_0_9(reader, *dest, true));
    if ((c = peek_byte(reader)) == '.') {
      has_decimal = true;

      // Annoyingly, dot can be end of statement, so tentatively eat
      skip_byte(reader, c);
      c = peek_byte(reader);
      if (!is_digit(c) && c != 'e' && c != 'E') {
        *ate_dot = true;     // Force caller to deal with stupid grammar
        return SERD_SUCCESS; // Next byte is not a number character
      }

      push_byte(reader, *dest, '.');
      read_0_9(reader, *dest, false);
    }
  }
  c = peek_byte(reader);
  if (c == 'e' || c == 'E') {
    // double
    push_byte(reader, *dest, eat_byte_safe(reader, c));
    switch ((c = peek_byte(reader))) {
    case '+':
    case '-':
      push_byte(reader, *dest, eat_byte_safe(reader, c));
      break;
    default:
      break;
    }
    TRY(st, read_0_9(reader, *dest, true));
    *datatype = push_node(reader, SERD_URI, XSD_DOUBLE, sizeof(XSD_DOUBLE) - 1);
  } else if (has_decimal) {
    *datatype =
      push_node(reader, SERD_URI, XSD_DECIMAL, sizeof(XSD_DECIMAL) - 1);
  } else {
    *datatype =
      push_node(reader, SERD_URI, XSD_INTEGER, sizeof(XSD_INTEGER) - 1);
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_iri(SerdReader* const reader, Ref* const dest, bool* const ate_dot)
{
  switch (peek_byte(reader)) {
  case '<':
    return read_IRIREF(reader, dest);
  default:
    *dest = push_node(reader, SERD_CURIE, "", 0);
    return read_PrefixedName(reader, *dest, true, ate_dot);
  }
}

static SerdStatus
read_literal(SerdReader* const    reader,
             Ref* const           dest,
             Ref* const           datatype,
             Ref* const           lang,
             SerdNodeFlags* const flags,
             bool* const          ate_dot)
{
  *dest = push_node(reader, SERD_LITERAL, "", 0);

  SerdStatus st = read_String(reader, *dest, flags);
  if (st) {
    *dest = pop_node(reader, *dest);
    return st;
  }

  switch (peek_byte(reader)) {
  case '@':
    skip_byte(reader, '@');
    if ((st = read_LANGTAG(reader, lang))) {
      *datatype = pop_node(reader, *datatype);
      *lang     = pop_node(reader, *lang);
      *dest     = pop_node(reader, *dest);
      return r_err(reader, st, "bad language tag\n");
    }
    break;
  case '^':
    skip_byte(reader, '^');
    if (!eat_byte_check(reader, '^')) {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "expected '^'\n");
    }

    if ((st = read_iri(reader, datatype, ate_dot))) {
      *datatype = pop_node(reader, *datatype);
      *lang     = pop_node(reader, *lang);
      *dest     = pop_node(reader, *dest);
      return r_err(reader, st, "bad datatype\n");
    }
    break;
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_verb(SerdReader* const reader, Ref* const dest)
{
  if (peek_byte(reader) == '<') {
    return read_IRIREF(reader, dest);
  }

  /* Either a qname, or "a".  Read the prefix first, and if it is in fact
     "a", produce that instead.
  */
  *dest = push_node(reader, SERD_CURIE, "", 0);

  SerdStatus st      = read_PN_PREFIX(reader, *dest);
  bool       ate_dot = false;
  SerdNode*  node    = deref(reader, *dest);
  const int  next    = peek_byte(reader);
  if (!st && node->n_bytes == 1 && node->buf[0] == 'a' && next != ':' &&
      !is_PN_CHARS_BASE((uint32_t)next)) {
    pop_node(reader, *dest);
    *dest = push_node(reader, SERD_URI, NS_RDF "type", 47);
    return SERD_SUCCESS;
  }

  if (st > SERD_FAILURE ||
      (st = read_PrefixedName(reader, *dest, false, &ate_dot)) || ate_dot) {
    *dest = pop_node(reader, *dest);
    st    = st > SERD_FAILURE ? st : SERD_ERR_BAD_SYNTAX;
    return r_err(reader, st, "bad verb\n");
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_BLANK_NODE_LABEL(SerdReader* const reader,
                      Ref* const        dest,
                      bool* const       ate_dot)
{
  skip_byte(reader, '_');
  if (!eat_byte_check(reader, ':')) {
    return SERD_ERR_BAD_SYNTAX;
  }

  const Ref ref = *dest =
    push_node(reader,
              SERD_BLANK,
              reader->bprefix ? (char*)reader->bprefix : "",
              reader->bprefix_len);

  int c = peek_byte(reader); // First: (PN_CHARS | '_' | [0-9])
  if (is_digit(c) || c == '_') {
    push_byte(reader, ref, eat_byte_safe(reader, c));
  } else if (read_PN_CHARS(reader, ref)) {
    *dest = pop_node(reader, *dest);
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "invalid name start\n");
  }

  while ((c = peek_byte(reader))) { // Middle: (PN_CHARS | '.')*
    if (c == '.') {
      push_byte(reader, ref, eat_byte_safe(reader, c));
    } else if (read_PN_CHARS(reader, ref)) {
      break;
    }
  }

  SerdNode* n = deref(reader, ref);
  if (n->buf[n->n_bytes - 1] == '.' && read_PN_CHARS(reader, ref)) {
    // Ate trailing dot, pop it from stack/node and inform caller
    --n->n_bytes;
    serd_stack_pop(&reader->stack, 1);
    *ate_dot = true;
  }

  if (fancy_syntax(reader)) {
    if (is_digit(n->buf[reader->bprefix_len + 1])) {
      if ((n->buf[reader->bprefix_len]) == 'b') {
        ((char*)n->buf)[reader->bprefix_len] = 'B'; // Prevent clash
        reader->seen_genid                   = true;
      } else if (reader->seen_genid && n->buf[reader->bprefix_len] == 'B') {
        *dest = pop_node(reader, *dest);
        return r_err(reader,
                     SERD_ERR_ID_CLASH,
                     "found both 'b' and 'B' blank IDs, prefix required\n");
      }
    }
  }

  return SERD_SUCCESS;
}

static Ref
read_blankName(SerdReader* const reader)
{
  if (skip_byte(reader, '=') || eat_byte_check(reader, '=') != '=') {
    r_err(reader, SERD_ERR_BAD_SYNTAX, "expected '='\n");
    return 0;
  }

  Ref  subject = 0;
  bool ate_dot = false;
  read_ws_star(reader);
  read_iri(reader, &subject, &ate_dot);
  return subject;
}

static SerdStatus
read_anon(SerdReader* const reader,
          ReadContext       ctx,
          const bool        subject,
          Ref* const        dest)
{
  skip_byte(reader, '[');

  const SerdStatementFlags old_flags = *ctx.flags;
  const bool               empty     = peek_delim(reader, ']');

  if (subject) {
    *ctx.flags |= empty ? SERD_EMPTY_S : SERD_ANON_S_BEGIN;
  } else {
    *ctx.flags |= empty ? SERD_EMPTY_O : SERD_ANON_O_BEGIN;
    if (peek_delim(reader, '=')) {
      if (!(*dest = read_blankName(reader)) || !eat_delim(reader, ';')) {
        return SERD_ERR_BAD_SYNTAX;
      }
    }
  }

  if (!*dest) {
    *dest = blank_id(reader);
  }

  // Emit statement with this anonymous object first
  SerdStatus st = SERD_SUCCESS;
  if (ctx.subject) {
    TRY(st, emit_statement(reader, ctx, *dest, 0, 0));
  }

  // Switch the subject to the anonymous node and read its description
  ctx.subject = *dest;
  if (!empty) {
    *ctx.flags &= ~(unsigned)SERD_LIST_CONT;
    if (!subject) {
      *ctx.flags |= SERD_ANON_CONT;
    }

    bool ate_dot_in_list = false;
    TRY_FAILING(st, read_predicateObjectList(reader, ctx, &ate_dot_in_list));

    if (ate_dot_in_list) {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "'.' inside blank\n");
    }

    read_ws_star(reader);
    if (reader->end_sink) {
      reader->end_sink(reader->handle, deref(reader, *dest));
    }

    *ctx.flags = old_flags;
  }

  return st > SERD_FAILURE                      ? st
         : (eat_byte_check(reader, ']') == ']') ? SERD_SUCCESS
                                                : SERD_ERR_BAD_SYNTAX;
}

/* If emit is true: recurses, calling statement_sink for every statement
   encountered, and leaves stack in original calling state (i.e. pops
   everything it pushes). */
static SerdStatus
read_object(SerdReader* const  reader,
            ReadContext* const ctx,
            const bool         emit,
            bool* const        ate_dot)
{
  static const char* const XSD_BOOLEAN     = NS_XSD "boolean";
  static const size_t      XSD_BOOLEAN_LEN = 40;

#ifndef NDEBUG
  const size_t orig_stack_size = reader->stack.size;
#endif

  SerdStatus st = SERD_FAILURE;

  bool      simple   = (ctx->subject != 0);
  SerdNode* node     = NULL;
  Ref       o        = 0;
  Ref       datatype = 0;
  Ref       lang     = 0;
  uint32_t  flags    = 0;
  const int c        = peek_byte(reader);
  if (!fancy_syntax(reader)) {
    switch (c) {
    case '"':
    case ':':
    case '<':
    case '_':
      break;
    default:
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "expected: ':', '<', or '_'\n");
    }
  }
  switch (c) {
  case EOF:
  case ')':
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "expected object\n");
  case '[':
    simple = false;
    st     = read_anon(reader, *ctx, false, &o);
    break;
  case '(':
    simple = false;
    st     = read_collection(reader, *ctx, &o);
    break;
  case '_':
    st = read_BLANK_NODE_LABEL(reader, &o, ate_dot);
    break;
  case '<':
  case ':':
    st = read_iri(reader, &o, ate_dot);
    break;
  case '+':
  case '-':
  case '.':
  case '0':
  case '1':
  case '2':
  case '3':
  case '4':
  case '5':
  case '6':
  case '7':
  case '8':
  case '9':
    st = read_number(reader, &o, &datatype, ate_dot);
    break;
  case '\"':
  case '\'':
    st = read_literal(reader, &o, &datatype, &lang, &flags, ate_dot);
    break;
  default:
    /* Either a boolean literal, or a qname.  Read the prefix first, and if
       it is in fact a "true" or "false" literal, produce that instead.
    */
    o = push_node(reader, SERD_CURIE, "", 0);
    while (!read_PN_CHARS_BASE(reader, o)) {
    }
    node = deref(reader, o);
    if ((node->n_bytes == 4 && !memcmp(node->buf, "true", 4)) ||
        (node->n_bytes == 5 && !memcmp(node->buf, "false", 5))) {
      node->type = SERD_LITERAL;
      datatype   = push_node(reader, SERD_URI, XSD_BOOLEAN, XSD_BOOLEAN_LEN);
      st         = SERD_SUCCESS;
    } else if (read_PN_PREFIX_tail(reader, o) > SERD_FAILURE) {
      st = SERD_ERR_BAD_SYNTAX;
    } else {
      if ((st = read_PrefixedName(reader, o, false, ate_dot))) {
        st = st > SERD_FAILURE ? st : SERD_ERR_BAD_SYNTAX;
        pop_node(reader, o);
        return r_err(reader, st, "expected prefixed name\n");
      }
    }
  }

  if (!st && simple && o) {
    deref(reader, o)->flags = flags;
  }

  if (!st && emit && simple) {
    st = emit_statement(reader, *ctx, o, datatype, lang);
  } else if (!st && !emit) {
    ctx->object   = o;
    ctx->datatype = datatype;
    ctx->lang     = lang;
    return SERD_SUCCESS;
  }

  pop_node(reader, lang);
  pop_node(reader, datatype);
  pop_node(reader, o);
#ifndef NDEBUG
  assert(reader->stack.size == orig_stack_size);
#endif
  return st;
}

static SerdStatus
read_objectList(SerdReader* const reader, ReadContext ctx, bool* const ate_dot)
{
  SerdStatus st = SERD_SUCCESS;
  TRY(st, read_object(reader, &ctx, true, ate_dot));
  if (!fancy_syntax(reader) && peek_delim(reader, ',')) {
    return r_err(
      reader, SERD_ERR_BAD_SYNTAX, "syntax does not support abbreviation\n");
  }

  while (!*ate_dot && eat_delim(reader, ',')) {
    st = read_object(reader, &ctx, true, ate_dot);
  }

  return st;
}

static SerdStatus
read_predicateObjectList(SerdReader* const reader,
                         ReadContext       ctx,
                         bool* const       ate_dot)
{
  SerdStatus st = SERD_SUCCESS;
  while (!(st = read_verb(reader, &ctx.predicate)) && read_ws_star(reader) &&
         !(st = read_objectList(reader, ctx, ate_dot))) {
    ctx.predicate = pop_node(reader, ctx.predicate);
    if (*ate_dot) {
      return SERD_SUCCESS;
    }

    bool ate_semi = false;
    int  c        = 0;
    do {
      read_ws_star(reader);
      switch (c = peek_byte(reader)) {
      case EOF:
        return r_err(reader, SERD_ERR_BAD_SYNTAX, "unexpected end of file\n");
      case '.':
      case ']':
      case '}':
        return SERD_SUCCESS;
      case ';':
        skip_byte(reader, c);
        ate_semi = true;
      }
    } while (c == ';');

    if (!ate_semi) {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "missing ';' or '.'\n");
    }
  }

  ctx.predicate = pop_node(reader, ctx.predicate);
  return st;
}

static SerdStatus
end_collection(SerdReader* const reader,
               const ReadContext ctx,
               const Ref         n1,
               const Ref         n2,
               const SerdStatus  st)
{
  pop_node(reader, n2);
  pop_node(reader, n1);
  *ctx.flags &= ~(unsigned)SERD_LIST_CONT;
  if (!st) {
    return (eat_byte_check(reader, ')') == ')') ? SERD_SUCCESS
                                                : SERD_ERR_BAD_SYNTAX;
  }

  return st;
}

static SerdStatus
read_collection(SerdReader* const reader, ReadContext ctx, Ref* const dest)
{
  SerdStatus st = SERD_SUCCESS;
  skip_byte(reader, '(');

  bool end = peek_delim(reader, ')');

  *dest = end ? reader->rdf_nil : blank_id(reader);
  if (ctx.subject) { // Reading a collection object
    *ctx.flags |= (end ? 0 : SERD_LIST_O_BEGIN);
    TRY(st, emit_statement(reader, ctx, *dest, 0, 0));
    *ctx.flags &= SERD_LIST_O_BEGIN;
    *ctx.flags |= SERD_LIST_CONT;
  } else { // Reading a collection subject
    *ctx.flags |= (end ? 0 : SERD_LIST_S_BEGIN);
  }

  if (end) {
    return end_collection(reader, ctx, 0, 0, st);
  }

  /* The order of node allocation here is necessarily not in stack order,
     so we create two nodes and recycle them throughout. */
  Ref n1   = push_node_padded(reader, genid_size(reader), SERD_BLANK, "", 0);
  Ref n2   = 0;
  Ref node = n1;
  Ref rest = 0;

  ctx.subject = *dest;
  while (!peek_delim(reader, ')')) {
    // _:node rdf:first object
    ctx.predicate = reader->rdf_first;
    bool ate_dot  = false;
    if ((st = read_object(reader, &ctx, true, &ate_dot)) || ate_dot) {
      return end_collection(reader, ctx, n1, n2, st);
    }

    if (!(end = peek_delim(reader, ')'))) {
      /* Give rest a new ID.  Done as late as possible to ensure it is
         used and > IDs generated by read_object above. */
      if (!rest) {
        rest = n2 = blank_id(reader); // First pass, push
      } else {
        set_blank_id(reader, rest, genid_size(reader));
      }
    }

    // _:node rdf:rest _:rest
    *ctx.flags |= SERD_LIST_CONT;
    ctx.predicate = reader->rdf_rest;
    TRY(st, emit_statement(reader, ctx, (end ? reader->rdf_nil : rest), 0, 0));

    ctx.subject = rest;        // _:node = _:rest
    rest        = node;        // _:rest = (old)_:node
    node        = ctx.subject; // invariant
  }

  return end_collection(reader, ctx, n1, n2, st);
}

static SerdStatus
read_subject(SerdReader* const reader,
             const ReadContext ctx,
             Ref* const        dest,
             int* const        s_type)
{
  SerdStatus st      = SERD_SUCCESS;
  bool       ate_dot = false;
  switch ((*s_type = peek_byte(reader))) {
  case '[':
    st = read_anon(reader, ctx, true, dest);
    break;
  case '(':
    st = read_collection(reader, ctx, dest);
    break;
  case '_':
    st = read_BLANK_NODE_LABEL(reader, dest, &ate_dot);
    break;
  default:
    st = read_iri(reader, dest, &ate_dot);
  }

  if (ate_dot) {
    pop_node(reader, *dest);
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "subject ends with '.'\n");
  }

  return st;
}

static SerdStatus
read_labelOrSubject(SerdReader* const reader, Ref* const dest)
{
  bool ate_dot = false;
  switch (peek_byte(reader)) {
  case '[':
    skip_byte(reader, '[');
    read_ws_star(reader);
    if (!eat_byte_check(reader, ']')) {
      return SERD_ERR_BAD_SYNTAX;
    }
    *dest = blank_id(reader);
    return SERD_SUCCESS;
  case '_':
    return read_BLANK_NODE_LABEL(reader, dest, &ate_dot);
  default:
    if (!read_iri(reader, dest, &ate_dot)) {
      return SERD_SUCCESS;
    } else {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "expected label or subject\n");
    }
  }
}

static SerdStatus
read_triples(SerdReader* const reader, ReadContext ctx, bool* const ate_dot)
{
  SerdStatus st = SERD_FAILURE;
  if (ctx.subject) {
    read_ws_star(reader);
    switch (peek_byte(reader)) {
    case '.':
      *ate_dot = eat_byte_safe(reader, '.');
      return SERD_FAILURE;
    case '}':
      return SERD_FAILURE;
    }
    st = read_predicateObjectList(reader, ctx, ate_dot);
  }

  ctx.subject = ctx.predicate = 0;
  return st > SERD_FAILURE ? st : SERD_SUCCESS;
}

static SerdStatus
read_base(SerdReader* const reader, const bool sparql, const bool token)
{
  SerdStatus st = SERD_SUCCESS;
  if (token) {
    TRY(st, eat_string(reader, "base", 4));
  }

  read_ws_star(reader);

  Ref uri = 0;
  TRY(st, read_IRIREF(reader, &uri));
  if (reader->base_sink) {
    TRY(st, reader->base_sink(reader->handle, deref(reader, uri)));
  }
  pop_node(reader, uri);

  read_ws_star(reader);
  if (!sparql) {
    return eat_byte_check(reader, '.') ? SERD_SUCCESS : SERD_ERR_BAD_SYNTAX;
  }

  if (peek_byte(reader) == '.') {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "full stop after SPARQL BASE\n");
  }

  return SERD_SUCCESS;
}

static SerdStatus
read_prefixID(SerdReader* const reader, const bool sparql, const bool token)
{
  SerdStatus st = SERD_SUCCESS;
  if (token) {
    TRY(st, eat_string(reader, "prefix", 6));
  }

  read_ws_star(reader);
  Ref name = push_node(reader, SERD_LITERAL, "", 0);
  TRY_FAILING(st, read_PN_PREFIX(reader, name));

  if (eat_byte_check(reader, ':') != ':') {
    pop_node(reader, name);
    return SERD_ERR_BAD_SYNTAX;
  }

  read_ws_star(reader);
  Ref uri = 0;
  TRY(st, read_IRIREF(reader, &uri));

  if (reader->prefix_sink) {
    st = reader->prefix_sink(
      reader->handle, deref(reader, name), deref(reader, uri));
  }

  pop_node(reader, uri);
  pop_node(reader, name);
  if (!sparql) {
    read_ws_star(reader);
    st = eat_byte_check(reader, '.') ? SERD_SUCCESS : SERD_ERR_BAD_SYNTAX;
  }

  return st;
}

static SerdStatus
read_directive(SerdReader* const reader)
{
  const bool sparql = peek_byte(reader) != '@';
  if (!sparql) {
    skip_byte(reader, '@');
    switch (peek_byte(reader)) {
    case 'B':
    case 'P':
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "uppercase directive\n");
    }
  }

  switch (peek_byte(reader)) {
  case 'B':
  case 'b':
    return read_base(reader, sparql, true);
  case 'P':
  case 'p':
    return read_prefixID(reader, sparql, true);
  default:
    break;
  }

  return r_err(reader, SERD_ERR_BAD_SYNTAX, "invalid directive\n");
}

static SerdStatus
read_wrappedGraph(SerdReader* const reader, ReadContext* const ctx)
{
  if (!eat_byte_check(reader, '{')) {
    return SERD_ERR_BAD_SYNTAX;
  }

  read_ws_star(reader);
  while (peek_byte(reader) != '}') {
    bool ate_dot  = false;
    int  s_type   = 0;
    ctx->subject  = 0;
    SerdStatus st = read_subject(reader, *ctx, &ctx->subject, &s_type);
    if (st) {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "bad subject\n");
    }

    if ((st = read_triples(reader, *ctx, &ate_dot)) && s_type != '[') {
      return r_err(reader, st, "bad predicate object list\n");
    }

    ctx->subject = pop_node(reader, ctx->subject);
    read_ws_star(reader);
    if (peek_byte(reader) == '.') {
      skip_byte(reader, '.');
    }
    read_ws_star(reader);
  }

  skip_byte(reader, '}');
  read_ws_star(reader);
  if (peek_byte(reader) == '.') {
    return r_err(reader, SERD_ERR_BAD_SYNTAX, "graph followed by '.'\n");
  }

  return SERD_SUCCESS;
}

static int
tokcmp(SerdReader* const reader,
       const Ref         ref,
       const char* const tok,
       const size_t      n)
{
  SerdNode* node = deref(reader, ref);
  if (!node || node->n_bytes != n) {
    return -1;
  }

  return serd_strncasecmp((const char*)node->buf, tok, n);
}

SerdStatus
read_n3_statement(SerdReader* const reader)
{
#ifndef NDEBUG
  const size_t orig_stack_size = reader->stack.size;
#endif

  SerdStatementFlags flags   = 0;
  ReadContext        ctx     = {0, 0, 0, 0, 0, 0, &flags};
  bool               ate_dot = false;
  int                s_type  = 0;
  SerdStatus         st      = SERD_SUCCESS;
  read_ws_star(reader);
  switch (peek_byte(reader)) {
  case '\0':
    skip_byte(reader, '\0');
    return SERD_FAILURE;
  case EOF:
    return SERD_FAILURE;
  case '@':
    if (!fancy_syntax(reader)) {
      return r_err(
        reader, SERD_ERR_BAD_SYNTAX, "syntax does not support directives\n");
    }
    TRY(st, read_directive(reader));
    read_ws_star(reader);
    break;
  case '{':
    if (reader->syntax == SERD_TRIG) {
      TRY(st, read_wrappedGraph(reader, &ctx));
      read_ws_star(reader);
    } else {
      return r_err(
        reader, SERD_ERR_BAD_SYNTAX, "syntax does not support graphs\n");
    }
    break;
  default:
    TRY_FAILING(st, read_subject(reader, ctx, &ctx.subject, &s_type));

    if (!tokcmp(reader, ctx.subject, "base", 4)) {
      st = read_base(reader, true, false);
    } else if (!tokcmp(reader, ctx.subject, "prefix", 6)) {
      st = read_prefixID(reader, true, false);
    } else if (!tokcmp(reader, ctx.subject, "graph", 5)) {
      ctx.subject = pop_node(reader, ctx.subject);
      read_ws_star(reader);
      TRY(st, read_labelOrSubject(reader, &ctx.graph));
      read_ws_star(reader);
      TRY(st, read_wrappedGraph(reader, &ctx));
      pop_node(reader, ctx.graph);
      ctx.graph = 0;
      read_ws_star(reader);
    } else if (!tokcmp(reader, ctx.subject, "true", 4) ||
               !tokcmp(reader, ctx.subject, "false", 5)) {
      return r_err(reader, SERD_ERR_BAD_SYNTAX, "expected subject\n");
    } else if (read_ws_star(reader) && peek_byte(reader) == '{') {
      if (s_type == '(' || (s_type == '[' && !*ctx.flags)) {
        return r_err(reader, SERD_ERR_BAD_SYNTAX, "invalid graph name\n");
      }
      ctx.graph   = ctx.subject;
      ctx.subject = 0;
      TRY(st, read_wrappedGraph(reader, &ctx));
      pop_node(reader, ctx.graph);
      read_ws_star(reader);
    } else if ((st = read_triples(reader, ctx, &ate_dot))) {
      if (st == SERD_FAILURE && s_type == '[') {
        return SERD_SUCCESS;
      }

      if (ate_dot) {
        return r_err(
          reader, SERD_ERR_BAD_SYNTAX, "unexpected end of statement\n");
      }

      return st > SERD_FAILURE ? st : SERD_ERR_BAD_SYNTAX;
    } else if (!ate_dot) {
      read_ws_star(reader);
      st = (eat_byte_check(reader, '.') == '.') ? SERD_SUCCESS
                                                : SERD_ERR_BAD_SYNTAX;
    }

    ctx.subject = pop_node(reader, ctx.subject);
    break;
  }

#ifndef NDEBUG
  assert(reader->stack.size == orig_stack_size);
#endif

  return st;
}

SerdStatus
serd_reader_skip_until_byte(SerdReader* const reader, const uint8_t byte)
{
  int c = peek_byte(reader);

  while (c != byte && c != EOF) {
    skip_byte(reader, c);
    c = peek_byte(reader);
  }

  return c == EOF ? SERD_FAILURE : SERD_SUCCESS;
}

SerdStatus
read_turtleTrigDoc(SerdReader* const reader)
{
  while (!reader->source.eof) {
    const SerdStatus st = read_n3_statement(reader);
    if (st > SERD_FAILURE) {
      if (reader->strict) {
        return st;
      }
      serd_reader_skip_until_byte(reader, '\n');
    }
  }

  return SERD_SUCCESS;
}

SerdStatus
read_nquads_statement(SerdReader* const reader)
{
  SerdStatus         st      = SERD_SUCCESS;
  SerdStatementFlags flags   = 0;
  ReadContext        ctx     = {0, 0, 0, 0, 0, 0, &flags};
  bool               ate_dot = false;
  int                s_type  = 0;

  read_ws_star(reader);
  if (peek_byte(reader) == EOF) {
    return SERD_FAILURE;
  }

  if (peek_byte(reader) == '@') {
    return r_err(
      reader, SERD_ERR_BAD_SYNTAX, "syntax does not support directives\n");
  }

  // subject predicate object
  if ((st = read_subject(reader, ctx, &ctx.subject, &s_type)) ||
      !read_ws_star(reader) || (st = read_IRIREF(reader, &ctx.predicate)) ||
      !read_ws_star(reader) ||
      (st = read_object(reader, &ctx, false, &ate_dot))) {
    return st;
  }

  if (!ate_dot) { // graphLabel?
    read_ws_star(reader);
    switch (peek_byte(reader)) {
    case '.':
      break;
    case '_':
      TRY(st, read_BLANK_NODE_LABEL(reader, &ctx.graph, &ate_dot));
      break;
    default:
      TRY(st, read_IRIREF(reader, &ctx.graph));
    }

    // Terminating '.'
    read_ws_star(reader);
    if (!eat_byte_check(reader, '.')) {
      return SERD_ERR_BAD_SYNTAX;
    }
  }

  TRY(st, emit_statement(reader, ctx, ctx.object, ctx.datatype, ctx.lang));

  pop_node(reader, ctx.graph);
  pop_node(reader, ctx.lang);
  pop_node(reader, ctx.datatype);
  pop_node(reader, ctx.object);

  return SERD_SUCCESS;
}

SerdStatus
read_nquadsDoc(SerdReader* const reader)
{
  SerdStatus st = SERD_SUCCESS;

  while (!reader->source.eof && !st) {
    st = read_nquads_statement(reader);
  }

  return st;
}

#if defined(__clang__) && __clang_major__ >= 10
_Pragma("clang diagnostic pop")
#endif
