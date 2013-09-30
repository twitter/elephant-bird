namespace java com.twitter.elephantbird.pig.test.thrift

enum KeyEnum {
  A,
  B,
  C
}

struct MapKeyTest {
  1: optional map<bool, i32> booleans
  2: optional map<byte, i32> bytes
  3: optional map<i16, i32> shorts
  4: optional map<i32, i32> ints
  5: optional map<i64, i32> longs
  6: optional map<double, i32> doubles
  7: optional map<KeyEnum, i32> enums
  8: optional map<string, i32> strings
  9: optional map<binary, i32> binaries
}
