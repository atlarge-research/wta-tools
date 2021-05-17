import math

import pandas as pd
import pyspark.sql.functions as F
from matplotlib.ticker import ScalarFormatter
from plotnine import *


def generate_cdf(spark_dataframe, column, output_path, hor_axis_label, vert_axis_label, show=False):
    def get_column(df):
        df = df.select(column).filter(
            F.col(column) >= 0).withColumnRenamed(column, "target")
        if df.count() > 1000:  # DF is too large, use sampling to get the distribution.
            # Get the 1000 permilles
            permilles = df.approxQuantile("target", [float(i) / 1000 for i in range(0, 1001)], 0.001)
            df = pd.DataFrame({"target": permilles})
            return df
        else:
            return df.toPandas().dropna()

    def normalize_df(df):
        df.sort_values(df.columns[0], inplace=True)
        if len(df) == 1:
            df["pdf"] = [1]
            df["cdf"] = [1]
        else:
            df["pdf"] = df["target"] / df["target"].sum()
            df["cdf"] = df["pdf"].cumsum()

    pdf = get_column(spark_dataframe)

    offset_text = ""

    if len(pdf) == 0:  # If the dataframe after filtering is empty, then print this metric is not available
        import matplotlib.pyplot as plt
        fig = plt.figure(figsize=(6, 3))
        ax = fig.add_subplot(111)
        ax.set_xlabel(hor_axis_label.format(offset_text), fontsize=18)
        ax.set_ylabel(vert_axis_label, fontsize=18)
        ax.text(0.5, 0.5, 'This trace does not contain this information.', horizontalalignment='center',
                verticalalignment='center', transform=ax.transAxes, fontsize=12)

        fig.savefig(output_path, dpi=600, format="png")
        return

    normalize_df(pdf)

    def normalize_group(gp):
        gp = gp.groupby("target").count().reset_index()[["target"]]
        normalize_df(gp)
        return gp

    count_df = normalize_group(pdf).reset_index()

    # Add a row at the start so that the CDF starts at 0 and ends at 1 (in case we only have one datapoint in the DF)
    count_df.index = count_df.index + 1  # shifting index
    count_df['index'] = count_df['index'] + 1
    count_df.loc[0] = [0, -math.inf, 0, 0]  # add a row  at the start (index, count, pdf, cdf)
    count_df.loc[len(count_df)] = [len(count_df), math.inf, 1, 1]
    count_df.sort_index(inplace=True)

    plt = ggplot(count_df) + \
          theme_light(base_size=18) + \
          geom_step(aes(x="target", y="cdf"), size=1)

    # Make the labels better readable
    fig = plt.draw()
    ax = fig.get_axes()[0]
    ax.get_xaxis().get_offset_text().set_visible(False)
    ax.get_xaxis().set_major_formatter(ScalarFormatter(useMathText=True))
    fig.tight_layout()  # Need to set this to be able to get the offset... for whatever reason
    offset_text = ax.get_xaxis().get_major_formatter().get_offset()

    ax.set_xlabel(hor_axis_label.format(f' {offset_text}' if offset_text else ""), fontsize=18)
    ax.set_ylabel(vert_axis_label, fontsize=18)
    ax.tick_params(axis='both', which='major', labelsize=16)
    ax.tick_params(axis='both', which='minor', labelsize=14)

    fig.savefig(output_path, format='png', dpi=600, figsize=(6, 3))

    if show:
        fig.show()
