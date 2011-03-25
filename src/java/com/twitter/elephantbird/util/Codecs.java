package com.twitter.elephantbird.util;

import java.lang.reflect.InvocationTargetException;

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
            return Base64.class.getConstructor(int.class).newInstance(0);
        } catch (SecurityException e) {
        } catch (NoSuchMethodException e) {
        } catch (IllegalArgumentException e) {
        } catch (InstantiationException e) {
        } catch (IllegalAccessException e) {
        } catch (InvocationTargetException e) {
        }
        return new Base64();
    }

}
