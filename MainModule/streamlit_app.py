import matplotlib.pyplot as plt
import streamlit as st
import mlflow
import pandas as pd
import seaborn as sns


def on_click_ok():
    # show the runs
    with st.expander(f"Found {runs.shape[0]} runs:"):
        st.dataframe(runs)
    fig = plt.figure(figsize=(9, 7))
    sns.boxplot(data=runs, x=selected_param, y=selected_metric)
    st.pyplot(fig)


@st.cache_data
def search_runs(experiment_ids) -> pd.DataFrame:
    runs = mlflow.search_runs(experiment_ids)
    # make numeric wherever possible
    runs = runs.apply(pd.to_numeric, errors="ignore")
    return runs


# Get Runs by experiment id
exp_options = [f"{e.experiment_id} '{e.name}'" for e in mlflow.search_experiments()]
selected_exp = st.sidebar.multiselect("Choose Experiment(s)", exp_options)
runs = search_runs([e.split(" ")[0] for e in selected_exp])

# select parameter
param_options = [col for col in runs.columns.to_list() if col.startswith("params")]
selected_param = st.sidebar.selectbox("Choose Parameter", param_options)

# select metric
metric_options = [col for col in runs.columns.to_list() if col.startswith("metrics")]
selected_metric = st.sidebar.selectbox("Choose Metric", metric_options)

st.sidebar.button("OK", on_click=on_click_ok)
