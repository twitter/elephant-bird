package com.twitter.elephantbird.mapreduce.input;

import static org.junit.Assert.*;

import com.twitter.elephantbird.mapreduce.io.DecodeException;
import org.junit.Test;
import java.nio.charset.Charset;

public class TestBase64Codec {

  private static final String TEST_STRING = "The quick brown fox jumps over the lazy dog.";
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte[] PLAIN = TEST_STRING.getBytes(UTF8);
  private static final byte[] ENCODED = Base64Codec.encodeToByte(PLAIN, false);

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] c = new byte[a.length + b.length];
    System.arraycopy(a, 0, c, 0, a.length);
    System.arraycopy(b, 0, c, a.length, b.length);
    return c;
  }

  @Test
  public void testDecode() throws DecodeException {
    assertArrayEquals(PLAIN, Base64Codec.decode(ENCODED));
  }

  @Test
  public void testDecodeIllegal() throws DecodeException {
    byte[] illegal = "%$%".getBytes(UTF8);
    byte[] merged = concat(ENCODED, concat(illegal, ENCODED));
    // illegal characters in the middle are not ignored
    assertFalse(concat(PLAIN, PLAIN).length == Base64Codec.decode(merged).length);
  }

  @Test
  public void testDecodeIllegalLeading() throws DecodeException {
    byte[] leading = "%$%".getBytes(UTF8);
    byte[] merged = concat(leading, ENCODED);
    assertArrayEquals(PLAIN, Base64Codec.decode(merged));
  }

  @Test
  public void testDecodeIllegalTrailing() throws DecodeException {
    byte[] trailing = "%$%".getBytes(UTF8);
    byte[] merged = concat(ENCODED, trailing);
    assertArrayEquals(PLAIN, Base64Codec.decode(merged));
  }

  @Test(expected=DecodeException.class)
  public void testDecodeInvalidLength() throws DecodeException {
    Base64Codec.decode(ENCODED, 0, ENCODED.length + 1); // incorrect length
  }

  // tests for the fast decode version:

  @Test
  public void testDecodeFast() throws DecodeException {
    assertArrayEquals(PLAIN, Base64Codec.decodeFast(ENCODED, ENCODED.length));
  }

  @Test
  public void testDecodeFastIllegal() throws DecodeException {
    byte[] illegal = "%$%".getBytes(UTF8);
    byte[] merged = concat(ENCODED, concat(illegal, ENCODED));
    // illegal characters in the middle are not ignored
    assertFalse(concat(PLAIN, PLAIN).length == Base64Codec.decode(merged).length);
  }

  @Test
  public void testDecodeFastIllegalLeading() throws DecodeException {
    byte[] leading = "%$%".getBytes(UTF8);
    byte[] merged = concat(leading, ENCODED);
    assertArrayEquals(PLAIN, Base64Codec.decodeFast(merged, merged.length));
  }

  @Test
  public void testDecodeFastIllegalTrailing() throws DecodeException {
    byte[] trailing = "%$%".getBytes(UTF8);
    byte[] merged = concat(ENCODED, trailing);
    assertArrayEquals(PLAIN, Base64Codec.decodeFast(merged, merged.length));
  }

  @Test
  public void testDecodeFastZeroLength() throws DecodeException {
    assertEquals(0, Base64Codec.decodeFast(new byte[0], 0).length);
  }

  @Test(expected=DecodeException.class)
  public void testDecodeFastNullWithNonZeroLength() throws DecodeException {
    Base64Codec.decodeFast(null, 100);
  }

  @Test(expected=DecodeException.class)
  public void testDecodeFastInvalidLength() throws DecodeException {
    Base64Codec.decodeFast(ENCODED, ENCODED.length + 1); // incorrect length
  }
}
