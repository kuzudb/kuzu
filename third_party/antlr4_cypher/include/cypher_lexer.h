
// Generated from src/antlr4/Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, COPY = 46, FROM = 47, NODE = 48, TABLE = 49, DROP = 50, 
    ALTER = 51, DEFAULT = 52, ADD = 53, COLUMN = 54, PRIMARY = 55, KEY = 56, 
    REL = 57, TO = 58, EXPLAIN = 59, PROFILE = 60, UNION = 61, ALL = 62, 
    OPTIONAL = 63, MATCH = 64, UNWIND = 65, CREATE = 66, SET = 67, DELETE = 68, 
    WITH = 69, RETURN = 70, DISTINCT = 71, STAR = 72, AS = 73, ORDER = 74, 
    BY = 75, L_SKIP = 76, LIMIT = 77, ASCENDING = 78, ASC = 79, DESCENDING = 80, 
    DESC = 81, WHERE = 82, OR = 83, XOR = 84, AND = 85, NOT = 86, INVALID_NOT_EQUAL = 87, 
    MINUS = 88, FACTORIAL = 89, STARTS = 90, ENDS = 91, CONTAINS = 92, IS = 93, 
    NULL_ = 94, TRUE = 95, FALSE = 96, EXISTS = 97, CASE = 98, ELSE = 99, 
    END = 100, WHEN = 101, THEN = 102, StringLiteral = 103, EscapedChar = 104, 
    DecimalInteger = 105, HexLetter = 106, HexDigit = 107, Digit = 108, 
    NonZeroDigit = 109, NonZeroOctDigit = 110, ZeroDigit = 111, RegularDecimalReal = 112, 
    UnescapedSymbolicName = 113, IdentifierStart = 114, IdentifierPart = 115, 
    EscapedSymbolicName = 116, SP = 117, WHITESPACE = 118, Comment = 119, 
    Unknown = 120
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

