package com.twitter.elephantbird.util;


import static org.junit.Assert.assertArrayEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCodecs {

    @Before
    public void setUp() {
    }


    @After
    public void tearDown() {
    }
    
    @Test
    public void testcreateStandardBase64() {
        String quote = "Man is distinguished, not only by his reason, but " +
        		"by this singular passion from other animals, which is a" +
        		" lust of the mind, that by a perseverance of delight in" +
        		" the continued and indefatigable generation of knowledge," +
        		" exceeds the short vehemence of any carnal pleasure.";
        String expected = "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpc" +
        		"yByZWFzb24sIGJ1dCBieSB0aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSB" +
        		"vdGhlci" +
                "BhbmltYWxzLCB3aGljaCBpcyBhIGx1c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYn" +
                "kgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0aGUgY29udGlu" +
                "dWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xl" +
                "ZGdlLCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5" +
                "hbCBwbGVhc3VyZS4="; 
        assertArrayEquals(Codecs.createStandardBase64().encode(quote.getBytes()),
                    expected.getBytes());
    }

}
