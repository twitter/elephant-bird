register ../../../dist/elephant-bird-1.0.jar;

-- the schema does not need to be explicitly specified. Pig queries the loader
-- for the schema. Specifying the schema in comments here helps the readers of
-- the script. It is commented out so that it is does not override the most
-- up to date schema when the Thrift class changes.
-- you use ThriftToPig class to pretty print :
--     $ java -cp "[..]elephant-bird-1.0.jar" com.twitter.elephantbird.pig.piggybank.ThriftToPig thrift.class.name

raw_data = load '/path/to/input_files' using com.twitter.elephantbird.pig.load.LzoThriftB64LinePigLoader('com.twitter.elephantbird.examples.thrift.Person');
       --  as (
       --       name: chararray,
       --       id: int,
       --       email: chararray,
       --       phones: {
       --         phones_tuple: (
       --           number: chararray,
       --           type: chararray
       --         )
       --       }
       --     )

person_phone_numbers = foreach raw_data generate name, FLATTEN(phone.phone_tuple.number) as phone_number;

phones_by_person = group person_phone_numbers by name;

person_phone_count = foreach phones_by_person generate group as name, COUNT(person_phone_numbers) as phone_count;

dump person_phone_count;


