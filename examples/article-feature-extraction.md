# Article Feature Extraction Example

### Input records

```
{"id": "1", "title": "The Case of the ...", "content": "..."}
{"id": "2", "title": "Unicyclist Breaks World Record ...", "content": "..."}
...
```

### Prompt

```
Extract features from the article "${title}" with content:

${content}
```

### Target Schema

```
{
  "type": "record",
  "name": "RandomEventArticle",
  "namespace": "com.example.events",
  "doc": "Schema for structured features extracted from random event articles.",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "event_category", "type": "string" },
    { "name": "main_subject", "type": "string" },
    { "name": "key_elements", "type": { "type": "array", "items": "string" } },
    { "name": "setting_location", "type": "string" },
    { "name": "cause_or_trigger", "type": "string" },
    { "name": "outcome_summary", "type": "string" },
    { "name": "mood_or_tone", "type": "string" }
  ]
}
```

Where `id` is an invariant.

### Resulting records

```
{
  "id" : "6",
  "event_category" : "Accident",
  "main_subject" : "Truck carrying brightly colored plastic balls",
  "key_elements" : [ "commuters", "traffic", "cleanup crews" ],
  "setting_location" : "Busy highway",
  "cause_or_trigger" : "A truck overturned",
  "outcome_summary" : "A truck spill created an impromptu art installation on a highway, halting traffic and bringing unexpected joy before cleanup crews restored order.",
  "mood_or_tone" : "Whimsical"
}
{
  "id" : "1",
  "event_category" : "Mischief",
  "main_subject" : "Gnorman, the garden gnome",
  "key_elements" : [ "Mrs. Gable", "local police" ],
  "setting_location" : "Town's water tower",
  "cause_or_trigger" : "The disappearance of Mrs. Gable's garden gnome",
  "outcome_summary" : "The gnome was found atop the town's water tower with a 'Free At Last!' sign, sparking a local debate on gnome liberation.",
  "mood_or_tone" : "Whimsical"
}
...
```
