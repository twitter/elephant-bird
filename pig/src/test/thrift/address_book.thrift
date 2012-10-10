namespace java com.twitter.elephantbird.pig.test.thrift

enum PhoneType {
  MOBILE = 0,
  HOME = 1,
  WORK = 2
}

struct PhoneNumber {
  1: string number,
  2: optional PhoneType type
}

struct Name {
  1: string first_name,
  2: string last_name
}

struct Person {
  1: required Name name,
  2: i32 id,
  3: string email,
  4: list<PhoneNumber> phones
}

struct AddressBook {
  1: list<Person> persons
}
