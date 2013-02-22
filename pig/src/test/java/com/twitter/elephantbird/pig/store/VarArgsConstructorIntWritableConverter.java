package com.twitter.elephantbird.pig.store;

import com.twitter.elephantbird.pig.util.IntWritableConverter;

/**
 * Simple WritableConverter impl which has no default constructor-- String arguments are required.
 *
 * @author Andy Schlaikjer
 */
public class VarArgsConstructorIntWritableConverter extends IntWritableConverter {
  private final String[] args;

  public VarArgsConstructorIntWritableConverter(String... args) {
    this.args = args;
  }

  public String[] getArgs() {
    return args;
  }
}
