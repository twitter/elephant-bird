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

struct TestIngredient {
  1: string name,
  2: string color,
}

struct TestRecipe {
  1: string name,
  2: list<TestIngredient> ingredients,
}

struct TestUniqueRecipe {
  1: string name,
  2: set<TestIngredient> ingredients,
}

struct TestNameList {
  1: string name,
  2: list<string> names,
}

struct TestNameSet {
  1: string name,
  2: set<string> names,
}

struct TestListInList {
  1: string name,
  2: list<list<string>> names,
}

struct TestSetInList {
  1: string name,
  2: list<set<string>> names,
}

struct TestListInSet {
  1: string name,
  2: set<list<string>> names,
}

struct TestSetInSet {
  1: string name,
  2: set<set<string>> names,
}
