-- we have to hard code the query literals because pig substitution via pigserver can't handle this kind of substitution
hits = load '$INPUT' using com.twitter.elephantbird.pig.PigLuceneIndexingIntegrationTest\$Loader('--queries','+(macbeth achilles)','+shield','+out, +"candle!"');
store hits into '$OUTPUT' using PigStorage('\t');
