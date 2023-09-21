
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
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, COMMENT = 49, MACRO = 50, 
    GLOB = 51, COPY = 52, FROM = 53, COLUMN = 54, NODE = 55, TABLE = 56, 
    GROUP = 57, RDF = 58, GRAPH = 59, DROP = 60, ALTER = 61, DEFAULT = 62, 
    RENAME = 63, ADD = 64, PRIMARY = 65, KEY = 66, REL = 67, TO = 68, EXPLAIN = 69, 
    PROFILE = 70, BEGIN = 71, TRANSACTION = 72, READ = 73, WRITE = 74, COMMIT = 75, 
    COMMIT_SKIP_CHECKPOINT = 76, ROLLBACK = 77, ROLLBACK_SKIP_CHECKPOINT = 78, 
    UNION = 79, ALL = 80, LOAD = 81, OPTIONAL = 82, MATCH = 83, UNWIND = 84, 
    CREATE = 85, MERGE = 86, ON = 87, SET = 88, DELETE = 89, WITH = 90, 
    RETURN = 91, DISTINCT = 92, STAR = 93, AS = 94, ORDER = 95, BY = 96, 
    L_SKIP = 97, LIMIT = 98, ASCENDING = 99, ASC = 100, DESCENDING = 101, 
    DESC = 102, WHERE = 103, SHORTEST = 104, OR = 105, XOR = 106, AND = 107, 
    NOT = 108, INVALID_NOT_EQUAL = 109, MINUS = 110, FACTORIAL = 111, STARTS = 112, 
    ENDS = 113, CONTAINS = 114, IS = 115, NULL_ = 116, TRUE = 117, FALSE = 118, 
    EXISTS = 119, CASE = 120, ELSE = 121, END = 122, WHEN = 123, THEN = 124, 
    StringLiteral = 125, EscapedChar = 126, DecimalInteger = 127, HexLetter = 128, 
    HexDigit = 129, Digit = 130, NonZeroDigit = 131, NonZeroOctDigit = 132, 
    ZeroDigit = 133, RegularDecimalReal = 134, UnescapedSymbolicName = 135, 
    IdentifierStart = 136, IdentifierPart = 137, EscapedSymbolicName = 138, 
    SP = 139, WHITESPACE = 140, Comment = 141, Unknown = 142
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

