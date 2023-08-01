
// Generated from Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, MACRO = 49, GLOB = 50, 
    COPY = 51, FROM = 52, NPY = 53, COLUMN = 54, NODE = 55, TABLE = 56, 
    DROP = 57, ALTER = 58, DEFAULT = 59, RENAME = 60, ADD = 61, PRIMARY = 62, 
    KEY = 63, REL = 64, TO = 65, EXPLAIN = 66, PROFILE = 67, UNION = 68, 
    ALL = 69, OPTIONAL = 70, MATCH = 71, UNWIND = 72, CREATE = 73, MERGE = 74, 
    ON = 75, SET = 76, DELETE = 77, WITH = 78, RETURN = 79, DISTINCT = 80, 
    STAR = 81, AS = 82, ORDER = 83, BY = 84, L_SKIP = 85, LIMIT = 86, ASCENDING = 87, 
    ASC = 88, DESCENDING = 89, DESC = 90, WHERE = 91, SHORTEST = 92, OR = 93, 
    XOR = 94, AND = 95, NOT = 96, INVALID_NOT_EQUAL = 97, MINUS = 98, FACTORIAL = 99, 
    STARTS = 100, ENDS = 101, CONTAINS = 102, IS = 103, NULL_ = 104, TRUE = 105, 
    FALSE = 106, EXISTS = 107, CASE = 108, ELSE = 109, END = 110, WHEN = 111, 
    THEN = 112, StringLiteral = 113, EscapedChar = 114, DecimalInteger = 115, 
    HexLetter = 116, HexDigit = 117, Digit = 118, NonZeroDigit = 119, NonZeroOctDigit = 120, 
    ZeroDigit = 121, RegularDecimalReal = 122, UnescapedSymbolicName = 123, 
    IdentifierStart = 124, IdentifierPart = 125, EscapedSymbolicName = 126, 
    SP = 127, WHITESPACE = 128, Comment = 129, Unknown = 130
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

