import itertools

import numpy as np
import pandas as pd

OUTPUT_FILENAME = "output_pandas.csv"


def main():
    dataset1_df = pd.read_csv("dataset1.csv")
    dataset2_df = pd.read_csv("dataset2.csv", index_col="counter_party")

    agg_df = (
        # join our two datasets together, bringing in the tier column
        dataset1_df.join(dataset2_df, on="counter_party")
        # pandas NamedAggs don't support filtering, so we create temp pre-filtered
        # columns to simplify the logic
        .assign(
            ARAP_value=np.where(dataset1_df["status"] == "ARAP", dataset1_df["value"], 0),
            ACCR_value=np.where(dataset1_df["status"] == "ACCR", dataset1_df["value"], 0),
        )
        .groupby(["legal_entity", "counter_party", "tier"], as_index=False)
        .agg(
            max_rating=("rating", np.max),
            ARAP_total_value=("ARAP_value", np.sum),
            ACCR_total_value=("ACCR_value", np.sum),
        )
    )

    # generate dataframes with subtotals for each grouping level
    dfs = []
    dims = ["legal_entity", "counter_party", "tier"]
    metrics = ["max_rating", "ARAP_total_value", "ACCR_total_value"]

    # generate all possible combinations with 1 <= length < 3 of the 3 dimensions
    # these will be our grouping sets for each subtotal group
    for i in range(1, len(dims)):
        for subset in itertools.combinations(dims, i):
            temp_df = (
                agg_df
                # group by out grouping set
                .groupby(list(subset), as_index=False).agg(
                    max_rating=("max_rating", np.max),
                    ARAP_total_value=("ARAP_total_value", np.sum),
                    ACCR_total_value=("ACCR_total_value", np.sum),
                )
                # add missing columns and set their value to "Total"
                .assign(**{k: "Total" for k in set(dims) - set(subset)})
            )
            # append our subtotal dataframe to the list, enduring correct column order
            dfs.append(temp_df[dims + metrics])

    # append/UNION subtotal dataframes to our main dataframe
    out_df = pd.concat([agg_df] + dfs)

    # save our output to csv format
    out_df.to_csv("output_pandas.csv", sep=",", index=False)
    print(f"output saved to {OUTPUT_FILENAME}")


if __name__ == "__main__":
    main()
