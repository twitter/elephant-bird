register ../../../dist/elephant-bird-1.0.jar;

raw_data = load '/path/to/input_files' using com.twitter.elephantbird.examples.proto.pig.load.LzoPersonProtobufBlockPigLoader()
      as (
        name: chararray,
        id: int,
        email: chararray,
        phone: bag {
          phone_tuple: tuple (
              number: chararray,
              type: chararray
          )
        }
      );

person_phone_numbers = foreach raw_data generate name, FLATTEN(phone.phone_tuple.number) as phone_number;

phones_by_person = group person_phone_numbers by name;

person_phone_count = foreach phones_by_person generate group as name, COUNT(person_phone_numbers) as phone_count;

dump person_phone_count;


