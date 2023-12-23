
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, T__45 = 46, CALL = 47, COMMENT = 48, MACRO = 49, GLOB = 50, 
    COPY = 51, FROM = 52, COLUMN = 53, NODE = 54, TABLE = 55, GROUP = 56, 
    RDF = 57, GRAPH = 58, DROP = 59, ALTER = 60, DEFAULT = 61, RENAME = 62, 
    ADD = 63, PRIMARY = 64, KEY = 65, REL = 66, TO = 67, EXPLAIN = 68, PROFILE = 69, 
    BEGIN = 70, TRANSACTION = 71, READ = 72, ONLY = 73, WRITE = 74, COMMIT = 75, 
    COMMIT_SKIP_CHECKPOINT = 76, ROLLBACK = 77, ROLLBACK_SKIP_CHECKPOINT = 78, 
    EXTENSION = 79, UNION = 80, ALL = 81, LOAD = 82, HEADERS = 83, OPTIONAL = 84, 
    MATCH = 85, UNWIND = 86, CREATE = 87, MERGE = 88, ON = 89, SET = 90, 
    DETACH = 91, DELETE = 92, WITH = 93, RETURN = 94, DISTINCT = 95, STAR = 96, 
    AS = 97, ORDER = 98, BY = 99, L_SKIP = 100, LIMIT = 101, ASCENDING = 102, 
    ASC = 103, DESCENDING = 104, DESC = 105, WHERE = 106, SHORTEST = 107, 
    OR = 108, XOR = 109, AND = 110, NOT = 111, INVALID_NOT_EQUAL = 112, 
    MINUS = 113, FACTORIAL = 114, STARTS = 115, ENDS = 116, CONTAINS = 117, 
    IS = 118, NULL_ = 119, TRUE = 120, FALSE = 121, COUNT = 122, EXISTS = 123, 
    CASE = 124, ELSE = 125, END = 126, WHEN = 127, THEN = 128, StringLiteral = 129, 
    EscapedChar = 130, DecimalInteger = 131, HexLetter = 132, HexDigit = 133, 
    Digit = 134, NonZeroDigit = 135, NonZeroOctDigit = 136, ZeroDigit = 137, 
    RegularDecimalReal = 138, UnescapedSymbolicName = 139, IdentifierStart = 140, 
    IdentifierPart = 141, EscapedSymbolicName = 142, SP = 143, WHITESPACE = 144, 
    Comment = 145, Unknown = 146
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

