import streamlit as st
import mlflow
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns


DEFAULT_FIG_SIZE = (6, 5)
DOWNSAMPLING_OPTIONS = ["TO_MIN_GROUP_SIZE", "None", "Exact Value"]
# KEY = 0  # keys for enumerating widgets to avoid DuplicateWidgetID error
st.set_page_config(layout="wide")


def get_runs(exp_ids):
    runs = mlflow.search_runs(exp_ids)
    runs = runs[~runs["params.subset_selection"].isnull()]  # drop parent runs
    runs = runs.apply(pd.to_numeric, errors="ignore")  # make numeric wherever possible
    runs = runs.astype({"params.size": 'int'})
    return runs


def select_metrics(runs, default=None, col=None, key=None):
    metric_options = [col for col in runs.columns.to_list() if col.startswith("metrics")]
    if default is None:
        default = ["metrics.precision", "metrics.recall", "metrics.fscore"]
    else:
        default = metric_options
    col = st if col is None else col
    return col.multiselect("Choose Metrics", metric_options, key=key,
                           default=default)


def fig_size_sliders(key=None):
    global fig
    st.write("Figure Size")
    size_x = st.sidebar.slider(label="X", min_value=5, max_value=20, value=DEFAULT_FIG_SIZE[0], key=key)
    size_y = st.sidebar.slider(label="Y", min_value=3, max_value=15, value=DEFAULT_FIG_SIZE[1], key=key)
    fig = plt.figure(figsize=(size_x, size_y))


def basic_box_plot(runs, param, chart_arrangement, fig, x_order=None, x_label=None, key=None, rotate_xticks=0):
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
        bp = sns.boxplot(data=runs, x=param, y=metric, order=x_order, color='skyblue')
        bp.set_xlabel(bp.get_xlabel().split('.')[-1] if x_label is None else x_label)
        bp.set_ylabel(bp.get_ylabel().split('.')[-1])
        skip_xlabels(bp, 10)
        plt.xticks(rotation=rotate_xticks)
        col.pyplot(fig)
        plt.clf()


def skip_xlabels(bp, number):
    if len(bp.get_xticklabels()) > number:
        skip = round(len(bp.get_xticklabels()) / number)
        for ind, label in enumerate(bp.get_xticklabels()):
            if ind % skip == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)


def colormap(runs, x, y, fig, chart_arrangement, key):
    st.write(f"**{len(runs)}** Runs | sizes from "
             f"**{int(runs['params.size'].min())}** to **{int(runs['params.size'].max())}**")
    cmap = "cool_r"
    metrics = select_metrics(runs, key=key)
    if not metrics:
        st.write("No metrics selected.")
        return
    cols = st.columns(len(metrics))
    for col, metric in zip(cols, metrics):
        if chart_arrangement == "Vertical":
            col = st
        metric_values = runs[metric]
        colormap = sns.scatterplot(x=x, y=y, data=runs, c=metric_values, cmap=cmap, legend=False)
        colormap.set_xlabel(colormap.get_xlabel().split('.')[-1])
        colormap.set_ylabel(colormap.get_ylabel().split('.')[-1])
        # color bar
        norm = plt.Normalize(metric_values.min(), metric_values.max())
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        ax = fig.axes[0]
        cax = fig.add_axes([ax.get_position().x1 + 0.05, ax.get_position().y0, 0.06, ax.get_position().height / 2])
        ax.figure.colorbar(sm, cax=cax)
        plt.suptitle(metric.split('.')[-1])
        col.pyplot(fig)
        plt.clf()
