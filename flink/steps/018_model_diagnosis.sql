-- Prerequisite: Flink workspace Catalog + Database dropdowns must match your
-- environment and Kafka cluster (e.g. confluent-ai-day / cluster_0), OR use:
-- CREATE MODEL `your_env`.`your_cluster`.diagnosis_model ...
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
