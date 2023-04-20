import csv
import io
import itertools

import apache_beam as beam

DATASET1_HEADER = ["invoice_id", "legal_entity", "counter_party", "rating", "status", "value"]
DATASET2_HEADER = ["counter_party", "tier"]
OUTPUT_HEADER = [
    "legal_entity",
    "counter_party",
    "tier",
    "max_rating",
    "ARAP_total_value",
    "ACCR_total_value",
]
OUTPUT_FILENAME = "output_beam"
NEWLINES = b"\r"


class ParseCSV(beam.DoFn):
    def process(self, element, fieldnames):
        reader = csv.DictReader([element], fieldnames=fieldnames)
        for row in reader:
            yield row


class WriteCSV(beam.DoFn):
    """Use python's builtin csv module to properly format a csv row"""

    def process(self, element):
        csv_row = io.StringIO()
        # lineterminator is set to empty because beam.io.WriteToText takes care of that for us
        writer = csv.writer(csv_row, lineterminator="")
        writer.writerow(element)

        yield csv_row.getvalue()


class UnpackJoinedRows(beam.DoFn):
    """Generates a flattened dict from a CoGroupByKey join"""

    def process(self, element):
        _, rows = element

        for row in rows[0]:
            yield {**row, **rows[1][0]}


class UnpackListofLists(beam.DoFn):
    """Generates a flattened tuple from a CoGroupByKey join"""

    def process(self, element):
        key, row = element

        yield (key, tuple(item for sublist in row for item in sublist))


class ExplodeSubtotals(beam.DoFn):
    """Generates all subtotal groupings/combinations for a given row/key"""

    def process(self, element):
        key, rows = element

        for i in range(1, len(key)):
            for subset in itertools.combinations(key, i):
                new_key = [n if n in list(subset) else "Total" for n in key]
                yield (tuple(new_key), rows)


# main pipeline
# provided files are terminated with CR, so we need to set delimiter correctly when
# reading CSVs
with beam.Pipeline() as pipeline:
    # read first dataset into PCollection
    dataset1 = (
        pipeline
        | "Read dataset1 CSV file"
        >> beam.io.ReadFromText("./dataset1.csv", delimiter=b"\r", skip_header_lines=1)
        | "Parse dataset1 CSV data" >> beam.ParDo(ParseCSV(), DATASET1_HEADER)
        | "Make dataset1 joinable by counter_party"
        >> beam.Map(lambda row: (row["counter_party"], row))
    )

    # read second dataset into PCollection
    dataset2 = (
        pipeline
        | "Read dataset2 CSV file"
        >> beam.io.ReadFromText("./dataset2.csv", delimiter=b"\r", skip_header_lines=1)
        | "Parse dataset2 CSV data" >> beam.ParDo(ParseCSV(), DATASET2_HEADER)
        | "Make dataset2 joinable by counter_party"
        >> beam.Map(lambda row: (row["counter_party"], row))
    )

    joined = (
        [dataset1, dataset2]
        | beam.CoGroupByKey()
        | "flatten joined rows" >> beam.ParDo(UnpackJoinedRows())
    )

    # group rows by legal_entity,counter_party,tier
    groupby = joined | "extract key from joined rows" >> beam.Map(
        lambda row: ((row["legal_entity"], row["counter_party"], row["tier"]), row)
    )

    # group rows by subtotal groupings of legal_entity,counter_party,tier
    subtotal_groupby = groupby | "explode subtotal groupings" >> beam.ParDo(ExplodeSubtotals())

    # union/flatten both PCollections together (regular rows + subtotal rows)
    flattened_groupby = [
        groupby,
        subtotal_groupby,
    ] | "merge groupby and subtotals groupby" >> beam.Flatten()

    max_rating = (
        flattened_groupby
        | "extract rating" >> beam.Map(lambda row: (row[0], int(row[1]["rating"])))
        | "calculate max rating" >> beam.CombinePerKey(max)
    )

    ARAP_total_value = (
        flattened_groupby
        | "extract ARAP value"
        >> beam.Map(
            lambda row: (row[0], int(row[1]["value"] if row[1]["status"] == "ARAP" else 0))
        )
        | "calculate ARAP total value" >> beam.CombinePerKey(sum)
    )

    ACCR_total_value = (
        flattened_groupby
        | "extract ACCR value"
        >> beam.Map(
            lambda row: (row[0], int(row[1]["value"] if row[1]["status"] == "ACCR" else 0))
        )
        | "calculate ACCR total value" >> beam.CombinePerKey(sum)
    )

    merged_metrics = (
        [max_rating, ARAP_total_value, ACCR_total_value]
        | "merge metrics PCollections" >> beam.CoGroupByKey()
        | "unpack list of lists of metrics" >> beam.ParDo(UnpackListofLists())
    )

    # write our csv file
    output = (
        merged_metrics
        | "flatten output rows" >> beam.Map(lambda x: list(itertools.chain.from_iterable(x)))
        | "format output rows" >> beam.ParDo(WriteCSV())
        | "write output csv"
        >> beam.io.WriteToText(
            OUTPUT_FILENAME,
            file_name_suffix=".csv",
            header=",".join(OUTPUT_HEADER),
            shard_name_template="",
        )
    )
