package com.twitter.elephantbird.crunch;

import com.google.common.collect.Sets;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.ReadableData;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Set;

import static org.apache.crunch.lib.Distinct.distinct;

/**
 * An example to demonstrate using Crunch to write data pipelines over Elephant Bird's example
 * protocol buffer messages.
 */
public class CrunchElephantBirdExample extends CrunchTool {

  /**
   * A {@link FilterFn} that reads in a path containing {@link AddressBook} messages and eliminates any
   * input {@link Person} messages that were not contained in at least one {@code AddressBook} message.
   */
  private static class PersonInAddressBookFn extends FilterFn<Person> {

    // A {@code Serializable} reference to the address book data on HDFS
    private ReadableData<AddressBook> addressData;
    private transient Set<Person> peopleInBook;

    public PersonInAddressBookFn(ReadableData<AddressBook> addressData) {
      this.addressData = addressData;
    }

    @Override
    public void configure(Configuration conf) {
      // Configures this MR job to copy the address book data from HDFS into the DistributedCache
      addressData.configure(conf);
    }

    @Override
    public void initialize() {
      // Load the set of matching people into memory by reading in the AddressBook data from
      // the DistributedCache
      peopleInBook = Sets.newHashSet();
      try {
        for (AddressBook ab : addressData.read(getContext())) {
          for (Person p : ab.getPersonList()) {
            peopleInBook.add(p);
          }
        }
      } catch (IOException e) {
        throw new CrunchRuntimeException("Error reading address book data", e);
      }
    }

    @Override
    public boolean accept(Person person) {
      return peopleInBook.contains(person);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: <address_book.proto> <people.proto> <output_dir>");
      return 1;
    }

    // Creates a reference to a collection of address books that we will read into memory in our
    // map tasks.
    PCollection<AddressBook> addresses = read(LzoProtobufSource.at(new Path(args[0]), AddressBook.class));

    // Processes a large collection of Person records, limiting it to those people who are found in
    // at least one of the AddressBook records. The address books are assumed to be small, and so we
    // pass a readable version of them into the function we use to process the Person records on the
    // map-side.
    PCollection<Person> found = read(LzoProtobufSource.at(new Path(args[1]), Person.class))
        .filter(new PersonInAddressBookFn(addresses.asReadable(false)));

    // Write the distinct Person records that made it past the address book filter to the given output directory
    // as protocol buffer records.
    write(distinct(found), new LzoProtobufTarget(new Path(args[2])));

    // Execute the pipeline and return a success indicator
    return done().succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new CrunchElephantBirdExample(), args));
  }
}
