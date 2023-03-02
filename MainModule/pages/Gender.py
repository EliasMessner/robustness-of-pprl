from streamlit_entry import *

key = 0


st.set_page_config(page_title="Gender")
st.write("# Gender")

CHART_ARRANGEMENT = st.sidebar.radio(label="Chart Arrangement", options=["Vertical", "Horizontal"])
st.sidebar.write("Figure Size")
key += 1
size_x = st.sidebar.slider(label="X", min_value=5, max_value=20, value=DEFAULT_FIG_SIZE[0], key=key)
key += 1
size_y = st.sidebar.slider(label="Y", min_value=3, max_value=15, value=DEFAULT_FIG_SIZE[1], key=key)
fig = plt.figure(figsize=(size_x, size_y))


def experiment_multiselect(default):
    options = [f"{e.experiment_id} '{e.name}'" for e in mlflow.search_experiments()]  # show ID and name
    default = [f"{_id} '{mlflow.get_experiment(_id).name}'" for _id in default]
    selection = st.multiselect("Experiments", options, default)
    return [value.split()[0] for value in selection]  # return only ID


exp_ids = experiment_multiselect(default=["382333411211593299"])
runs = get_runs(exp_ids)
assert (runs["params.subset_selection"] == "ATTRIBUTE_VALUE").all()
assert (runs["params.column"] == "GENDER").all()

with st.expander(f"Found {runs.shape[0]} runs:"):
    st.dataframe(runs)

with st.expander("Exact Value"):
    st.write("## Exact Value")
    param = "params.equals"
    runs_exact_val = runs[~runs[param].isnull()]
    st.write(f"**{len(runs_exact_val)}** Runs | sizes from "
             f"**{int(runs_exact_val['params.size'].min())}** to **{int(runs_exact_val['params.size'].max())}**")
    key += 1
    basic_box_plot(runs_exact_val, param=param, chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig)

with st.expander("Gender Distribution"):
    st.write("## Gender Distribution")
    param = "params.dist"
    runs_dist = runs[~runs[param].isnull()]
    key += 1
    basic_box_plot(runs_dist, param=param, x_order=sorted(runs_dist[param].unique().tolist()),
                   chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig)
