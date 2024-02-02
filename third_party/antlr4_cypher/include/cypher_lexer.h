
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
    T__44 = 45, CALL = 46, COMMENT = 47, MACRO = 48, GLOB = 49, COPY = 50, 
    FROM = 51, COLUMN = 52, NODE = 53, TABLE = 54, GROUP = 55, RDFGRAPH = 56, 
    DROP = 57, ALTER = 58, DEFAULT = 59, RENAME = 60, ADD = 61, PRIMARY = 62, 
    KEY = 63, REL = 64, TO = 65, EXPLAIN = 66, PROFILE = 67, BEGIN = 68, 
    TRANSACTION = 69, READ = 70, ONLY = 71, WRITE = 72, COMMIT = 73, COMMIT_SKIP_CHECKPOINT = 74, 
    ROLLBACK = 75, ROLLBACK_SKIP_CHECKPOINT = 76, INSTALL = 77, EXTENSION = 78, 
    UNION = 79, ALL = 80, LOAD = 81, HEADERS = 82, OPTIONAL = 83, MATCH = 84, 
    UNWIND = 85, CREATE = 86, MERGE = 87, ON = 88, SET = 89, DETACH = 90, 
    DELETE = 91, WITH = 92, RETURN = 93, DISTINCT = 94, STAR = 95, AS = 96, 
    ORDER = 97, BY = 98, L_SKIP = 99, LIMIT = 100, ASCENDING = 101, ASC = 102, 
    DESCENDING = 103, DESC = 104, WHERE = 105, SHORTEST = 106, OR = 107, 
    XOR = 108, AND = 109, NOT = 110, INVALID_NOT_EQUAL = 111, MINUS = 112, 
    FACTORIAL = 113, COLON = 114, IN = 115, STARTS = 116, ENDS = 117, CONTAINS = 118, 
    IS = 119, NULL_ = 120, TRUE = 121, FALSE = 122, COUNT = 123, EXISTS = 124, 
    CASE = 125, ELSE = 126, END = 127, WHEN = 128, THEN = 129, StringLiteral = 130, 
    EscapedChar = 131, DecimalInteger = 132, HexLetter = 133, HexDigit = 134, 
    Digit = 135, NonZeroDigit = 136, NonZeroOctDigit = 137, ZeroDigit = 138, 
    RegularDecimalReal = 139, UnescapedSymbolicName = 140, IdentifierStart = 141, 
    IdentifierPart = 142, EscapedSymbolicName = 143, SP = 144, WHITESPACE = 145, 
    Comment = 146, Unknown = 147
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

