package com.twitter.elephantbird.mapred.input;

public interface  DeprecatedInputFormatValueCopier<T> {
	public abstract void copyValue(T oldValue, T newValue);
}
