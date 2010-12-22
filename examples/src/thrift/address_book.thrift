namespace java com.twitter.elephantbird.examples.thrift

enum PhoneType {
  MOBILE = 0,
  HOME = 1,
  WORK = 2
}

struct PhoneNumber {
  1: string number,
  2: optional PhoneType type
}

struct Person {
  1: string name,
  2: i32 id,
  3: string email,
  4: list<PhoneNumber> phones
}

struct AddressBook {
  1: list<Person> persons
}
