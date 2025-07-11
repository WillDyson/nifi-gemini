# Content Summary Example

### Input records

```
{"id": "QsUJQkfM00ubh5Rgsac1B", "language": "french", "content": "The unpredictability of random numbers is fundamental ..."}
{"id": "2MC8CbVJiHNQ5XoK9rorD", "language": "english", "content": "When network connected devices communicate with each other, ..."}
{"id": "y6LnNruAteaFtPxdZDuGh", "language": "german", "content": "A mug, glass, or even bowl – if you want to be very continental ..."}
```

### Prompt

```
Summarise the following content in ${language}:

${content}
```

### Target Schema

```
{
  "type": "record",
  "name": "Summary",
  "namespace": "com.example",
  "fields": [
    {
      "name": "id",
      "type": "string"
    }
    {
      "name": "language",
      "type": "string"
    }
    {
      "name": "summary",
      "type": "string"
    }
  ]
}
```

Where `id` and `language` are invariants.

### Resulting records

```
{"id": "y6LnNruAteaFtPxdZDuGh", "language": "german", { "id" "summary" : "Heiße Schokolade ist ein ultimatives ..."}
{"id": "QsUJQkfM00ubh5Rgsac1B", "language": "french", "summary": "Les nombres aléatoires sont essentiels pour ..."}
{"id": "2MC8CbVJiHNQ5XoK9rorD", "language": "english", "summary": "Network protocols are essential rules enabling ..."}
```
