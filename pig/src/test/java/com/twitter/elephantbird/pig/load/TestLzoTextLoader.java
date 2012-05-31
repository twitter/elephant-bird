package com.twitter.elephantbird.pig.load;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.pig.data.DataByteArray;
import org.junit.Test;

public class TestLzoTextLoader {
  public static ArrayList<String[]> data = new ArrayList<String[]>();
  static {
    data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100", "\"-\"",
    "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
    data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100",
        "\"http://myreferringsite.com\"",
    "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
    data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100", "\"-\"",
    "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
  }

  public static ArrayList<DataByteArray[]> EXPECTED = new ArrayList<DataByteArray[]>();
  static {

    for (int i = 0; i < data.size(); i++) {
      ArrayList<DataByteArray> thisExpected = new ArrayList<DataByteArray>();
      for (int j = 0; j <= 2; j++) {
        thisExpected.add(new DataByteArray(data.get(i)[j]));
      }
      String temp = data.get(i)[3];
      temp = temp.replace("[", "");
      temp = temp.replace("]", "");
      thisExpected.add(new DataByteArray(temp));

      temp = data.get(i)[4];

      for (String thisOne : data.get(i)[4].split(" ")) {
        thisOne = thisOne.replace("\"", "");
        thisExpected.add(new DataByteArray(thisOne));
      }
      for (int j = 5; j <= 6; j++) {
        thisExpected.add(new DataByteArray(data.get(i)[j]));
      }
      for (int j = 7; j <= 8; j++) {
        String thisOne = data.get(i)[j];
        thisOne = thisOne.replace("\"", "");
        thisExpected.add(new DataByteArray(thisOne));
      }

      DataByteArray[] toAdd = new DataByteArray[0];
      toAdd = (thisExpected.toArray(toAdd));
      EXPECTED.add(toAdd);
    }
  }

  @Test
  public void needsRealTests() {
    assertTrue("needs real tests", true);
  }
  /*
    @Test
    public void testLoadFromBindTo() throws Exception {
        String filename = TestHelper.createTempFile(data, " ");
        CombinedLogLoader combinedLogLoader = new CombinedLogLoader();
        PigServer pigServer = new PigServer(LOCAL);
        InputStream inputStream = FileLocalizer.open(filename, pigServer.getPigContext());
        combinedLogLoader.bindTo(filename, new BufferedPositionedInputStream(inputStream), 0, Long.MAX_VALUE);

        int tupleCount = 0;

        while (true) {
            Tuple tuple = combinedLogLoader.getNext();
            if (tuple == null)
                break;
            else {
                TestHelper.examineTuple(EXPECTED, tuple, tupleCount);
                tupleCount++;
            }
        }
        assertEquals(data.size(), tupleCount);
    }

    public void testLoadFromPigServer() throws Exception {
        String filename = TestHelper.createTempFile(data, " ");
        PigServer pig = new PigServer(ExecType.LOCAL);
        filename = filename.replace("\\", "\\\\");
        pig.registerQuery("A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();");
        Iterator<?> it = pig.openIterator("A");

        int tupleCount = 0;

        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                TestHelper.examineTuple(EXPECTED, tuple, tupleCount);
                tupleCount++;
            }
        }
        assertEquals(data.size(), tupleCount);
    }
*/
}
