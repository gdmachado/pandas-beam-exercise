# Pandas & Beam exercise

This contains two solutions: one implemented using Pandas and other using Apache Beam (Python SDK).

## Problem statement

```text
using two input files dataset1 and dataset2 

join dataset1 with dataset2 and get tier

generate below output file

legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)

Also create new record to add total for each of legal entity, counterparty & tier.

Sample data:
legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)
L1,Total, Total, calculated_value, calculated_value,calculated_value
L1, C1, Total,calculated_value, calculated_value,calculated_value
Total,C1,Total,calculated_value, calculated_value,calculated_value
Total,Total,1,calculated_value, calculated_value,calculated_value
L2,Total,Total,calculated_value, calculated_value,calculated_value
....
like all other values.

where caluclated_value in sample data needs to be calculated using above method.
```

## How to run the solutions

Requirements:

- Python 3.10.11
- Pandas 2.0.0
- Apache Beam Python SDK 2.46.0

Initialize your environment (Mac OS):

```shell
python -m venv venv
./venv/bin/activate
pip install -r requirements.txt
```

Please note that on Windows you may need to initialize the virtualenvironment differently .

Run the Pandas solution:

```shell
python solution_pandas.py
```

Run the Apache Beam solution:

```shell
python solution_beam.py
```

## Description of solution

I've provided the generated output CSV files from both solutions with this ZIP file. Both solutions follow the same approach:

- Read both CSV files
- Join both files together by the `counter_party` key
- Merge them together to have the `tier` column as part of the dataset
- Aggregate the resulting dataset by `["legal_entity", "counter_party", "tier"]`
- Use `itertools.combinations` to generate the possible groupings (known as `PowerSet`) for generating subtotals (for example, `L1,Total,Total`, `L1,C1,Total` and so on)
- Use aggregation to calculate all subtotals
- Generate calculated metrics for these aggregations: `max(rating)`, `sum(value where status=ARAP)` and `sum(value where status=ACCR)`
- Save the output to a CSV file

Quick note: The CSV files are delimited with `CR`, since I generated these on a Mac, the output CSV files may appear with broken newlines or be incompatible on another system.

## Considerations

The Apache Beam solution is more complex than the Pandas solution, but obviously scales better on distributed systems. If I was writing these for a production server, it would make sense to turn the join with `dataset2` into a broadcast join to avoid unnecessary shuffles of the smaller dataset, assuming that dataset1 would be potentially huge.
