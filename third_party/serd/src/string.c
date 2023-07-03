// Copyright 2011-2020 David Robillard <d@drobilla.net>
// SPDX-License-Identifier: ISC

#include "string_utils.h"

#include "serd.h"

#include <math.h>
#include <stdint.h>
#include <stdlib.h>

void
serd_free(void* const ptr)
{
  free(ptr);
}

const uint8_t*
serd_strerror(const SerdStatus status)
{
  switch (status) {
  case SERD_SUCCESS:
    return (const uint8_t*)"Success";
  case SERD_FAILURE:
    return (const uint8_t*)"Non-fatal failure";
  case SERD_ERR_UNKNOWN:
    return (const uint8_t*)"Unknown error";
  case SERD_ERR_BAD_SYNTAX:
    return (const uint8_t*)"Invalid syntax";
  case SERD_ERR_BAD_ARG:
    return (const uint8_t*)"Invalid argument";
  case SERD_ERR_NOT_FOUND:
    return (const uint8_t*)"Not found";
  case SERD_ERR_ID_CLASH:
    return (const uint8_t*)"Blank node ID clash";
  case SERD_ERR_BAD_CURIE:
    return (const uint8_t*)"Invalid CURIE";
  case SERD_ERR_INTERNAL:
    return (const uint8_t*)"Internal error";
  case SERD_ERR_BAD_WRITE:
    return (const uint8_t*)"Error writing to file/stream";
  case SERD_ERR_BAD_TEXT:
    return (const uint8_t*)"Invalid text encoding";
  }
  return (const uint8_t*)"Unknown error"; // never reached
}

static void
serd_update_flags(const uint8_t c, SerdNodeFlags* const flags)
{
  switch (c) {
  case '\r':
  case '\n':
    *flags |= SERD_HAS_NEWLINE;
    break;
  case '"':
    *flags |= SERD_HAS_QUOTE;
    break;
  default:
    break;
  }
}

size_t
serd_substrlen(const uint8_t* const str,
               const size_t         len,
               size_t* const        n_bytes,
               SerdNodeFlags* const flags)
{
  size_t        n_chars = 0;
  size_t        i       = 0;
  SerdNodeFlags f       = 0;
  for (; i < len && str[i]; ++i) {
    if ((str[i] & 0xC0) != 0x80) { // Start of new character
      ++n_chars;
      serd_update_flags(str[i], &f);
    }
  }
  if (n_bytes) {
    *n_bytes = i;
  }
  if (flags) {
    *flags = f;
  }
  return n_chars;
}

size_t
serd_strlen(const uint8_t* const str,
            size_t* const        n_bytes,
            SerdNodeFlags* const flags)
{
  size_t        n_chars = 0;
  size_t        i       = 0;
  SerdNodeFlags f       = 0;
  for (; str[i]; ++i) {
    if ((str[i] & 0xC0) != 0x80) { // Start of new character
      ++n_chars;
      serd_update_flags(str[i], &f);
    }
  }
  if (n_bytes) {
    *n_bytes = i;
  }
  if (flags) {
    *flags = f;
  }
  return n_chars;
}

static double
read_sign(const char** const sptr)
{
  double sign = 1.0;

  switch (**sptr) {
  case '-':
    sign = -1.0;
    ++(*sptr);
    break;
  case '+':
    ++(*sptr);
    break;
  default:
    break;
  }

  return sign;
}

double
serd_strtod(const char* const str, char** const endptr)
{
  double result = 0.0;

  // Point s at the first non-whitespace character
  const char* s = str;
  while (is_space(*s)) {
    ++s;
  }

  // Read leading sign if necessary
  const double sign = read_sign(&s);

  // Parse integer part
  for (; is_digit(*s); ++s) {
    result = (result * 10.0) + (*s - '0');
  }

  // Parse fractional part
  if (*s == '.') {
    double denom = 10.0;
    for (++s; is_digit(*s); ++s) {
      result += (*s - '0') / denom;
      denom *= 10.0;
    }
  }

  // Parse exponent
  if (*s == 'e' || *s == 'E') {
    ++s;
    double expt      = 0.0;
    double expt_sign = read_sign(&s);
    for (; is_digit(*s); ++s) {
      expt = (expt * 10.0) + (*s - '0');
    }
    result *= pow(10, expt * expt_sign);
  }

  if (endptr) {
    *endptr = (char*)s;
  }

  return result * sign;
}
