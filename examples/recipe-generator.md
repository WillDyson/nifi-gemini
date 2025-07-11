# Recipe Generator Example

### Input records

```
{"id": "yCU8GgJ5LoW4JMzyFbw7v", "name": "pizza"}
{"id": "OX75UKpxOuRVvGJtb5sWF", "name": "pasta"}
...
```

### Prompt

```
Generate a recipe for ${name}
```

### Target Schema

```
{
  "type": "record",
  "name": "Recipe",
  "namespace": "com.example",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "name", "type": "string" },
    { "name": "ingredients",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Ingredient",
          "fields": [
            { "name": "name", "type": "string" },
            { "name": "unit", "type": "string" },
            { "name": "quantity", "type": "int" }
          ]
        }
      } },
    { "name": "steps", "type": { "type": "array", "items": "string" } },
    { "name": "emoji", "type": "string" }
  ]
}
```

Where `id` and `name` are invariants.

### Resulting records

```
{
  "id" : "yCU8GgJ5LoW4JMzyFbw7v",
  "name" : "pizza",
  "ingredients" : [ {
    "name" : "all-purpose flour",
    "unit" : "cups",
    "quantity" : 3
  }, {
    "name" : "warm water",
    "unit" : "cup",
    "quantity" : 1
  }, {
    "name" : "active dry yeast",
    "unit" : "teaspoon",
    "quantity" : 1
  }, {
    ...
  }
  ...
}
...
```
