package com.twitter.elephantbird.util;

public class Pair<A, B> {
  private final A first;
  private final B second;

  public Pair(A first, B second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public int hashCode() {
    int hashFirst = first != null ? first.hashCode() : 0;
    int hashSecond = second != null ? second.hashCode() : 0;
    return (hashFirst + hashSecond) * hashSecond + hashFirst;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Pair) {
      Pair otherPair = (Pair) other;
      if ((first == null && otherPair.first == null) ||
          (first != null && first.equals(otherPair.first))) {
        if ((second == null && otherPair.second == null) ||
            (second != null && second.equals(otherPair.second))) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "(" + first + ", " + second + ")";
  }

  public A getFirst() {
    return first;
  }

  public B getSecond() {
    return second;
  }
}
