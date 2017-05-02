/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

typedef TestName TestNameTypeDef

/* TestPerson, plus couple more traits */
struct TestPersonExtended {
  1: TestName                     name,
  2: map<TestPhoneType, string>   phones,
  3: string                       email,
  4: TestNameTypeDef              friend
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

struct TestMap {
  1: string name,
  2: map<string,string> names,
}

struct TestMapInList {
  1: string name,
  2: list<map<string,string>> names,
}

struct TestListInMap {
  1: string name,
  2: map<string,list<string>> names,
}

struct TestMapInSet {
  1: string name,
  2: set<map<string,string>> names,
}

struct TestSetInMap {
  1: string name,
  2: map<string,set<string>> names,
}

typedef map<string, i32> StringToIntMap

struct TestStructInMap {
  1: string name,
  2: map<string,TestPerson> names,
  3: StringToIntMap name_to_id
}

union TestUnion {
  1: string stringType,
  2: i32    i32Type,
  3: binary bufferType,
  4: TestName structType,
  5: bool boolType
}

struct TestBinaryInListMap {
  1: i32 count,
  2: list<map<string, binary>> binaryBlobs
}

exception TestException {
  1: string description
}

struct TestExceptionInMap {
  1: string name,
  2: map<string, TestException> exceptionMap
}

struct PrimitiveListsStruct {
  1: list<string> strings
  2: list<i64> longs
}

struct PrimitiveSetsStruct {
  1: set<string> strings
  2: set<i64> longs
}

struct MapStruct {
  1: map<i32, string> entries
}