CREATE TABLE manual_chunks_vec (
  `text`      STRING,
  `source`    STRING,
  asset_type  STRING,
  embedding   ARRAY<FLOAT>
) WITH (
  'connector'           = 'mongodb',
  'mongodb.connection'  = 'mongodb_connection',
  'mongodb.database'    = 'gridsentinel',
  'mongodb.collection'  = 'manual_chunks',
  'mongodb.index'       = 'manuals_vector_index',
  'mongodb.numcandidates' = '100'
);
