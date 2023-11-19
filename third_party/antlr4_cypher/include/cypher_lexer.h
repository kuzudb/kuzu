
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
    UNION = 79, ALL = 80, LOAD = 81, HEADERS = 82, OPTIONAL = 83, MATCH = 84, 
    UNWIND = 85, CREATE = 86, MERGE = 87, ON = 88, SET = 89, DETACH = 90, 
    DELETE = 91, WITH = 92, RETURN = 93, DISTINCT = 94, STAR = 95, AS = 96, 
    ORDER = 97, BY = 98, L_SKIP = 99, LIMIT = 100, ASCENDING = 101, ASC = 102, 
    DESCENDING = 103, DESC = 104, WHERE = 105, SHORTEST = 106, OR = 107, 
    XOR = 108, AND = 109, NOT = 110, INVALID_NOT_EQUAL = 111, MINUS = 112, 
    FACTORIAL = 113, STARTS = 114, ENDS = 115, CONTAINS = 116, IS = 117, 
    NULL_ = 118, TRUE = 119, FALSE = 120, COUNT = 121, EXISTS = 122, CASE = 123, 
    ELSE = 124, END = 125, WHEN = 126, THEN = 127, StringLiteral = 128, 
    EscapedChar = 129, DecimalInteger = 130, HexLetter = 131, HexDigit = 132, 
    Digit = 133, NonZeroDigit = 134, NonZeroOctDigit = 135, ZeroDigit = 136, 
    RegularDecimalReal = 137, UnescapedSymbolicName = 138, IdentifierStart = 139, 
    IdentifierPart = 140, EscapedSymbolicName = 141, SP = 142, WHITESPACE = 143, 
    Comment = 144, Unknown = 145
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

