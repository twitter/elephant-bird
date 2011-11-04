namespace java com.twitter.elephantbird.thrift.test

/**
 * various thrift classes used in unit tests
 */

enum TestPhoneType {
  MOBILE = 0,
  HOME = 1,
  WORK = 2
}

struct TestName {
  1: string first_name,
  2: string last_name
}

struct TestPerson {
  1: TestName                     name,
  2: map<TestPhoneType, string>   phones, // for testing enum keys in maps.
}

