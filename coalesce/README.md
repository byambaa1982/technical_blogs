## Understanding the PySpark `coalesce` Function: A Simple Guide

#### Problem Introduction

When working with big data using PySpark, you'll often find datasets with missing values (nulls) across several columns. Let’s say you have a table with user choices, but users could leave some choices empty. You want to find the user’s top preference by looking at their first, then second, then third choice in that order—choosing the first value that isn’t null.

How do you do this easily without a tangle of `when` and `otherwise` statements or custom Python code? That’s where the `coalesce` function comes in!

---

## What is `coalesce` in PySpark?

PySpark’s `coalesce` function lets you easily pick the first non-null value from a list of columns per row. It works just like the SQL COALESCE, but in your Spark DataFrame pipeline.

---

## Example Data

Let’s start with a sample DataFrame. Here’s what the data might look like in markdown:

```markdown
| username       | first_choice | second_choice | third_choice |
|----------------|-------------|---------------|--------------|
| Data Logic Hub | X           | facebook      | Instragram   |
| User 2         | NULL        | NULL          | NULL         |
| User 3         | facebook    | NULL          | Instragram   |
| User 4         | NULL        | X             | NULL         |
| User 5         | NULL        | NULL          | facebook     |
```

---

## How Not To Do It: Using Multiple `when` Statements

Before using `coalesce`, you might think about writing a bunch of `when` checks like this:

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "Choice",
    F.when(F.col("first_choice").isNotNull(), F.col("first_choice"))
     .when(F.col("second_choice").isNotNull(), F.col("second_choice"))
     .when(F.col("third_choice").isNotNull(), F.col("third_choice"))
)
```

This works, but can get messy if you have more columns. If you add a fourth or fifth choice, your code gets even longer and harder to read.

---

## Solution: Using `coalesce` (the Easy Way)

`coalesce` keeps your code neat, and it’s built for exactly this job. Here’s how it works:

```python
from pyspark.sql import functions as F

resultDF = df.withColumn(
    "Choice",
    F.coalesce(F.col("first_choice"), F.col("second_choice"), F.col("third_choice"))
)
resultDF.show()
```

---

### Output Table

```markdown
| username       | first_choice | second_choice | third_choice | Choice   |
|----------------|-------------|---------------|--------------|----------|
| Data Logic Hub | X           | facebook      | Instragram   | X        |
| User 2         | NULL        | NULL          | NULL         | NULL     |
| User 3         | facebook    | NULL          | Instragram   | facebook |
| User 4         | NULL        | X             | NULL         | X        |
| User 5         | NULL        | NULL          | facebook     | facebook |
```

---

## Why Use `coalesce`?

- **Clean and Simple:** One line replaces many.
- **Easy to Extend:** Just add more columns to the function, no need to keep adding more `when` clauses.
- **Works Fast:** Native Spark performance.
- **Handles Any Datatype:** Works for strings, numbers, etc.

---

## More Examples

### Example 1: Filling Default Values

Suppose your columns have `NULL`s and you want to make sure there’s always a fallback value.

```python
resultDF = df.withColumn(
    "FilledChoice",
    F.coalesce(F.col("first_choice"), F.col("second_choice"), F.lit("No Choice Given"))
)
resultDF.show()
```

**Output:**

```markdown
| username       | first_choice | second_choice | third_choice | FilledChoice     |
|----------------|-------------|---------------|--------------|------------------|
| Data Logic Hub | X           | facebook      | Instragram   | X                |
| User 2         | NULL        | NULL          | NULL         | No Choice Given  |
| User 3         | facebook    | NULL          | Instragram   | facebook         |
| User 4         | NULL        | X             | NULL         | X                |
| User 5         | NULL        | NULL          | facebook     | No Choice Given  |
```

---

### Example 2: Numeric Data

If you have columns like `score1`, `score2`, and `score3` and you want the first available score:

```python
df2 = spark.createDataFrame([
    ("A", None, 5, 10),
    ("B", 3, None, None),
    ("C", None, None, None)
], ["user", "score1", "score2", "score3"])

resultDF2 = df2.withColumn(
    "FirstScore",
    F.coalesce(F.col("score1"), F.col("score2"), F.col("score3"))
)
resultDF2.show()
```

**Output:**

```markdown
| user | score1 | score2 | score3 | FirstScore |
|------|--------|--------|--------|------------|
| A    | NULL   | 5      | 10     | 5          |
| B    | 3      | NULL   | NULL   | 3          |
| C    | NULL   | NULL   | NULL   | NULL       |
```

---

## Final Thoughts

Whenever you want to take the first non-null value from several columns and keep your logic simple, PySpark’s `coalesce` is your friend. It makes your ETL steps easier to read and maintain, helps you deal with missing data naturally, and is a common tool every PySpark user should understand.

**Remember:**
- Use `F.coalesce()` with as many columns as needed.
- Use `F.col("colname")` when referring to your columns inside the function.

---

**Happy Sparking!** 🚀