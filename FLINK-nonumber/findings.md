# Findings

## Problem

When performing a temporal join with:

- an `upsert-kafka` source as the input for the versioned table
- a temporary view that `select` from the source (e.g `transform` to add/modify a property)
- at least two insert statements
- at least one of the inserts are into a kafka (or custom sink)

then the join behaves as a LEFT JOIN even when the input data should match as an INNER JOIN.

## Symptom

The output will have `null` for the right side of the join (the versioned table contents)

## Workaround/fix

If possible, remove the temporary view. Otherwise, this is fixed in Flink 1.20.2 and 2.x
