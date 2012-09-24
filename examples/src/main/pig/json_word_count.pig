register ../../../dist/elephant-bird-1.0.jar;

-- To generate data for use with this pig script, take a json data file of the form
-- { "key1": 16, "key2": 1, "key3": 28 }
-- { "key4": 66, "key1": 1, "key5": 38, "key6": 77 }
-- ...
-- { "key82383": 29, "key1": 22 }
-- run lzop over it, and place the resulting compressed file in the directory you use
-- as the first argument to this class on the command line.
raw_data = load '/path/to/your_lzop_data' using com.twitter.elephantbird.pig.load.LzoJsonLoader()
            as (
              json: map[]
            );

certain_keys = foreach raw_data generate (int)json#'key1' as key1_count, (int)json#'key3' as key3_count;

-- etc.


