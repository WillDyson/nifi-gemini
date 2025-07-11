# Structured Output Gemini NiFi Processors

These NiFi processors allows you to process NiFi records through Vertex AI Gemini.

It uses batch inference and structured outputs to allow you:
- Perform a large number of inferences (one batch per flowfile, one inference per record)
- Force the returned records to conform to a schema that can be used downstream to process the results with record processors

It works in two steps, _SubmitGeminiBatchRecord_ prepares and submits the batch request. _PollGeminiBatchRecord_ then waits for that batch request to complete before outputting the results as a JSONL flowfile.

Examples of how these processors might be used can be found in the [examples](examples/) folder.
