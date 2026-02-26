hits = load '$INPUT' using com.twitter.elephantbird.pig.PigLuceneIndexingIntegrationTest\$Loader('--file', '$QUERY_FILE');
store hits into '$OUTPUT' using PigStorage('\t');
