package com.twitter.elephantbird.pig.util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.PigContext;

import com.google.common.collect.Lists;
import com.twitter.elephantbird.pig.load.SequenceFileLoader;
import com.twitter.elephantbird.pig.store.SequenceFileStorage;

/**
 * Configuration supporting Pig load and store function implementations for reading and writing
 * Hadoop {@link SequenceFile}s.
 *
 * @author Andy Schlaikjer
 * @see SequenceFileLoader
 * @see SequenceFileStorage
 * @see WritableConverter
 */
public class SequenceFileConfig<K extends Writable, V extends Writable> {
  public static final String CONVERTER_PARAM = "converter";
  public final CommandLine keyArguments;
  public final CommandLine valueArguments;
  public final CommandLine genericArguments;
  public final WritableConverter<K> keyConverter;
  public final WritableConverter<V> valueConverter;

  /**
   * Parses options from argument strings. Available options for key and value argument strings
   * include:
   *
   * <dl>
   * <dt>-c|--converter cls</dt>
   * <dd>{@link WritableConverter} implementation class to use for conversion of data. Defaults to
   * {@link TextConverter} for both key and value.</dd>
   * </dl>
   *
   * Any extra arguments found will be treated as String arguments for the WritableConverter
   * constructor. For instance, the argument string {@code "-c MyConverter 123 abc"} specifies
   * WritableConverter class {@code MyConverter} along with two constructor arguments {@code "123"}
   * and {@code "abc"}. This will cause SequenceFileLoader to attempt to invoke the following
   * constructors, in order, to create a new instance of MyConverter:
   *
   * <ol>
   * <li><code>MyConverter(String arg1, String arg2)</code> -- constructor arguments are passed as
   * explicit arguments.</li>
   * <li><code>MyConverter(String[] args)</code> -- constructor arguments are passed within a String
   * array.</li>
   * <li><code>MyConverter(String... args)</code> -- same as above, with var args syntax.</li>
   * <li><code>MyConverter(String argString)</code> -- constructor arguments are joined with space
   * char to create {@code argString}.</li>
   * </ol>
   *
   * If none of these constructors exist, a RuntimeException will be thrown.
   *
   * <p>
   * Note that WritableConverter constructor arguments prefixed by one or more hyphens will be
   * interpreted as options for SequenceFileLoader itself, resulting in an
   * {@link UnrecognizedOptionException}. To avoid this, place these values after a {@code --}
   * (double-hyphen) token:
   *
   * <pre>
   * A = LOAD '$data' USING com.twitter.elephantbird.pig.load.SequenceFileLoader (
   *   '-c ...IntWritableConverter',
   *   '-c ...MyComplexWritableConverter basic options here -- --complex -options here'
   * );
   * </pre>
   *
   * No generic options are exposed by default. {@link SequenceFileLoader} and
   * {@link SequenceFileStorage} may include more options.
   *
   * @param keyArgs argument string containing key options.
   * @param valueArgs argument string containing value options.
   * @param genericArgs argument string containing generic options.
   * @throws ParseException
   * @throws IOException
   */
  public SequenceFileConfig(String keyArgs, String valueArgs, String genericArgs)
      throws ParseException, IOException {
    // parse key, value arguments
    Options keyValueOptions = getKeyValueOptions();
    Options genericOptions = getGenericOptions();
    keyArguments = parseArguments(keyValueOptions, keyArgs);
    valueArguments = parseArguments(keyValueOptions, valueArgs);
    genericArguments = parseArguments(genericOptions, genericArgs);

    // construct key, value converters
    keyConverter = getWritableConverter(keyArguments);
    valueConverter = getWritableConverter(valueArguments);

    // initialize key, value converters
    initialize();
  }

  /**
   * Constructor without other arguments (backwards compatible).
   *
   * @throws ParseException
   * @throws IOException
   */
  public SequenceFileConfig(String keyArgs, String valueArgs) throws ParseException, IOException {
    this(keyArgs, valueArgs, "");
  }

  /**
   * Default constructor. Defaults used for all options.
   *
   * @throws ParseException
   * @throws IOException
   */
  public SequenceFileConfig() throws ParseException, IOException {
    this("", "");
  }

  /**
   * @return Options instance containing valid key/value options.
   */
  protected Options getKeyValueOptions() {
    @SuppressWarnings("static-access")
    Option converterOption =
        OptionBuilder
            .withLongOpt(CONVERTER_PARAM)
            .hasArg()
            .withArgName("cls")
            .withDescription(
                String.format("Converter type to use for conversion of data. Defaults to '%s'.",
                    TextConverter.class.getName())).create("c");
    return new Options().addOption(converterOption);
  }

  /**
   * @return Options instance containing valid global options.
   */
  protected Options getGenericOptions() {
    return new Options();
  }

  /**
   * @param args
   * @return CommandLine instance containing options parsed from argument string.
   * @throws ParseException
   */
  private static CommandLine parseArguments(Options options, String args) throws ParseException {
    CommandLine cmdline = null;
    try {
      cmdline = new GnuParser().parse(options, args.split(" "));
    } catch (ParseException e) {
      new HelpFormatter().printHelp(SequenceFileStorage.class.getName() + "(keyArgs, valueArgs)",
          options);
      throw e;
    }
    return cmdline;
  }

  /**
   * @param arguments
   * @return new WritableConverter instance constructed using given arguments.
   */
  @SuppressWarnings("unchecked")
  private static <T extends Writable> WritableConverter<T> getWritableConverter(
      CommandLine arguments) {
    // get remaining non-empty argument strings from commandline
    String[] converterArgs = removeEmptyArgs(arguments.getArgs());
    try {

      // get converter classname
      String converterClassName =
          arguments.getOptionValue(CONVERTER_PARAM, TextConverter.class.getName());

      // get converter class
      Class<WritableConverter<T>> converterClass =
          PigContext.resolveClassName(converterClassName);

      // construct converter instance
      if (converterArgs == null || converterArgs.length == 0) {

        // use default ctor
        return converterClass.newInstance();

      } else {
        try {

          // look up ctor having explicit number of String arguments
          Class<?>[] parameterTypes = new Class<?>[converterArgs.length];
          Arrays.fill(parameterTypes, String.class);
          Constructor<WritableConverter<T>> ctor = converterClass.getConstructor(parameterTypes);
          return ctor.newInstance((Object[]) converterArgs);

        } catch (NoSuchMethodException e) {
          try {

            // look up ctor having single String[] (or String... varargs) argument
            Constructor<WritableConverter<T>> ctor =
                converterClass.getConstructor(new Class<?>[] { String[].class });
            return ctor.newInstance((Object) converterArgs);

          } catch (NoSuchMethodException e2) {

            // look up ctor having single String argument and join args together
            Constructor<WritableConverter<T>> ctor =
                converterClass.getConstructor(new Class<?>[] { String.class });
            StringBuilder sb = new StringBuilder(converterArgs[0]);
            for (int i = 1; i < converterArgs.length; ++i) {
              sb.append(" ").append(converterArgs[i]);
            }
            return ctor.newInstance(sb.toString());

          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create WritableConverter instance", e);
    }
  }

  /**
   * @param args
   * @return new String[] containing non-empty values from args.
   */
  private static String[] removeEmptyArgs(String[] args) {
    List<String> converterArgsFiltered = Lists.newArrayList();
    for (String arg : args) {
      if (arg == null || arg.isEmpty())
        continue;
      converterArgsFiltered.add(arg);
    }
    return converterArgsFiltered.toArray(new String[0]);
  }

  /**
   * Initializes key, value WritableConverters.
   *
   * @throws IOException
   */
  protected void initialize() throws IOException {
    keyConverter.initialize(null);
    valueConverter.initialize(null);
  }
}
