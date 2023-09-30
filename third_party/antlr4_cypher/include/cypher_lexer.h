
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
    BEGIN = 70, TRANSACTION = 71, READ = 72, WRITE = 73, COMMIT = 74, COMMIT_SKIP_CHECKPOINT = 75, 
    ROLLBACK = 76, ROLLBACK_SKIP_CHECKPOINT = 77, UNION = 78, ALL = 79, 
    LOAD = 80, OPTIONAL = 81, MATCH = 82, UNWIND = 83, CREATE = 84, MERGE = 85, 
    ON = 86, SET = 87, DELETE = 88, WITH = 89, RETURN = 90, DISTINCT = 91, 
    STAR = 92, AS = 93, ORDER = 94, BY = 95, L_SKIP = 96, LIMIT = 97, ASCENDING = 98, 
    ASC = 99, DESCENDING = 100, DESC = 101, WHERE = 102, SHORTEST = 103, 
    OR = 104, XOR = 105, AND = 106, NOT = 107, INVALID_NOT_EQUAL = 108, 
    MINUS = 109, FACTORIAL = 110, STARTS = 111, ENDS = 112, CONTAINS = 113, 
    IS = 114, NULL_ = 115, TRUE = 116, FALSE = 117, EXISTS = 118, CASE = 119, 
    ELSE = 120, END = 121, WHEN = 122, THEN = 123, StringLiteral = 124, 
    EscapedChar = 125, DecimalInteger = 126, HexLetter = 127, HexDigit = 128, 
    Digit = 129, NonZeroDigit = 130, NonZeroOctDigit = 131, ZeroDigit = 132, 
    RegularDecimalReal = 133, UnescapedSymbolicName = 134, IdentifierStart = 135, 
    IdentifierPart = 136, EscapedSymbolicName = 137, SP = 138, WHITESPACE = 139, 
    Comment = 140, Unknown = 141
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

