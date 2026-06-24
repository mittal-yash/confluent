CREATE MODEL gemini_embed
INPUT  (input STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
  'provider'              = 'vertexai',
  'vertexai.connection'   = 'gemini_embed_connection',
  'vertexai.input_format' = 'VERTEX-EMBED',
  'task'                  = 'embedding'
);
