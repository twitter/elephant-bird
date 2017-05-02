package com.twitter.elephantbird.pig.store;

import com.twitter.elephantbird.pig.util.IntWritableConverter;

/**
 * Simple WritableConverter impl which has no default constructor-- String arguments are required.
 *
 * @author Andy Schlaikjer
 */
public class FixedArgsConstructorIntWritableConverter extends IntWritableConverter {
  private final String a;
  private final String b;

  public FixedArgsConstructorIntWritableConverter(String a, String b) {
    this.a = a;
    this.b = b;
  }

  public String getA() {
    return a;
  }

  public String getB() {
    return b;
  }
}
