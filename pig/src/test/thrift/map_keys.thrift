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
