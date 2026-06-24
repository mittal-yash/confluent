-- =====================================================================

-- 04 - Vertex AI Gemini connection + diagnosis model (AI Model Inference).

-- Diagnosis runs INSIDE Flink via ML_PREDICT/AI_COMPLETE in 05.

--

-- PREREQUISITE: GCP project with Vertex AI API enabled + a service account

-- with roles/aiplatform.user (or aiplatform.endpoints.predict). Paste the

-- service-account JSON into 'service-key' below — see infra/confluent_cloud_setup.md.

--

-- CLI alternative:

--   confluent flink connection create gemini_connection \

--     --cloud gcp --region asia-south1 --type vertexai \

--     --endpoint 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<PROJECT>/locations/asia-south1/publishers/google/models/gemini-2.5-flash' \

--     --service-key "$(cat vertex-sa.json)"

-- =====================================================================



CREATE CONNECTION gemini_connection

  WITH (

    'type'        = 'vertexai',

    'endpoint'    = 'https://asia-south1-aiplatform.googleapis.com/v1/projects/<YOUR_GCP_PROJECT>/locations/asia-south1/publishers/google/models/gemini-2.5-flash',

    'service-key' = '<YOUR_VERTEX_SERVICE_ACCOUNT_JSON>'

  );



CREATE MODEL diagnosis_model

INPUT  (`input` STRING)

OUTPUT (`output` STRING)

WITH (

  'provider'              = 'vertexai',

  'vertexai.connection'   = 'gemini_connection',

  'vertexai.input_format' = 'GEMINI-GENERATE',

  'task'                  = 'text_generation',

  'vertexai.system_prompt' =

    'You are a grid reliability engineer for a power utility. Diagnose the incident using ONLY the provided CONTEXT (equipment manuals + asset spec). Choose recommended_action from EXACTLY this set: ["monitor","throttle_load","reroute_load","dispatch_crew","isolate"]. Prefer the least disruptive action that removes the hazard (reroute/throttle before isolate); use reroute_load only if tie_assets are listed. If context is insufficient, recommend dispatch_crew. Respond with STRICT MINIFIED JSON ONLY (no markdown, no prose) with keys: root_cause (string), recommended_action (string from the set), rationale (string, cite the manual by name), confidence (number 0..1), citation (string = the manual/source name you used).'

);


