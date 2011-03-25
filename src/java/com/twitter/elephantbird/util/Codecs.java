package com.twitter.elephantbird.util;

import org.apache.commons.codec.binary.Base64;

/**
 * Various Codecs specific utilities.
 */
public final class Codecs {
    private Codecs() {
        
    }
    
    /**
     * Get a instance of standard base64 implementation from apache 
     * commons-codec library 
     * @return standard base64 instance
     */
    public static Base64 createStandardBase64() {
        try {
            Base64.class.getConstructor(int.class);
            return new Base64(0);
        } catch (SecurityException e) {
        } catch (NoSuchMethodException e) {
        }
        
        return new Base64();
    }

}
