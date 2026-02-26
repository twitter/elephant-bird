lines = load '$INPUT' using TextLoader() as (line:chararray);
store lines into '$OUTPUT' using  com.twitter.elephantbird.pig.store.LuceneIndexStorage('com.twitter.elephantbird.pig.PigLuceneIndexingIntegrationTest\$IndexOutputFormat');
