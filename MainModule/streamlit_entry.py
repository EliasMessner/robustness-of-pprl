import streamlit as st
import mlflow
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns


DEFAULT_FIG_SIZE = (9, 7)
DOWNSAMPLING_OPTIONS = ["TO_MIN_GROUP_SIZE", "None", "Exact Value"]
# KEY = 0  # keys for enumerating widgets to avoid DuplicateWidgetID error
st.set_page_config(layout="wide")


def get_runs(exp_ids):
    runs = mlflow.search_runs(exp_ids)
    runs = runs.apply(pd.to_numeric, errors="ignore")  # make numeric wherever possible
    runs = runs[~runs["params.subset_selection"].isnull()]  # drop parent runs
    return runs


def select_metrics(runs, all_selected=True, col=None, key=None):
    metric_options = [col for col in runs.columns.to_list() if col.startswith("metrics")]
    col = st if col is None else col
    return col.multiselect("Choose Metrics", metric_options, key=key,
                           default=["metrics.precision"] if not all_selected else metric_options)


def fig_size_sliders(key=None):
    global fig
    st.write("Figure Size")
    size_x = st.sidebar.slider(label="X", min_value=5, max_value=20, value=DEFAULT_FIG_SIZE[0], key=key)
    size_y = st.sidebar.slider(label="Y", min_value=3, max_value=15, value=DEFAULT_FIG_SIZE[1], key=key)
    fig = plt.figure(figsize=(size_x, size_y))


def basic_box_plot(runs, param, chart_arrangement, fig, x_order=None, key=None):
    # TODO allow filter by t ?
    st.write(f"**{len(runs)}** Runs | sizes from "
             f"**{int(runs['params.size'].min())}** to **{int(runs['params.size'].max())}**")
    metrics = select_metrics(runs, key=key)
    if not metrics:
        st.write("No metrics selected.")
        return
    cols = st.columns(len(metrics))
    for col, metric in zip(cols, metrics):
        if chart_arrangement == "Vertical":
            col = st
        sns.boxplot(data=runs, x=param, y=metric, order=x_order)
        col.pyplot(fig)
        plt.clf()
