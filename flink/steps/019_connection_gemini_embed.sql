CREATE CONNECTION gemini_embed_connection
  WITH (
    'type'        = 'vertexai',
    'endpoint'    = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<YOUR_GCP_PROJECT>/locations/asia-south1/publishers/google/models/gemini-embedding-001:predict',
    'service-key' = '<YOUR_VERTEX_SERVICE_ACCOUNT_JSON>'
  );
