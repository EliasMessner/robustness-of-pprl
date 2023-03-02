import itertools
from streamlit_entry import *

key = 0

st.set_page_config(page_title="Error Rate")
st.write("# Error Rate")

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


exp_ids = experiment_multiselect(default=["308707501760303082"])
runs = get_runs(exp_ids)
assert (runs["params.subset_selection"] == "ERROR_RATE").all()

step_selections = st.multiselect("Step", options=[2, 3], default=[2, 3])
preserve_overlap_selections = st.multiselect("Preserve Overlap", options=["True", "False"], default=["True", "False"])
downsampling_selections = st.multiselect("Downsampling", options=DOWNSAMPLING_OPTIONS, default=["TO_MIN_GROUP_SIZE"])


def resolve_downsampling(observed_value, selected_value):
    if selected_value == "Exact Value":
        return observed_value.isdigit()
    return observed_value == selected_value


for step, preserve_overlap, downsampling in itertools.product(step_selections, preserve_overlap_selections,
                                                              downsampling_selections):
    # TODO add step as param to dm_config and select by this param
    if step == 2:
        ranges = [[1, 2], [3, 4], [5, 6], [7, 8], [9, 12]]
    elif step == 3:
        ranges = [[1, 3], [4, 6], [7, 10]]
    else:
        raise ValueError()
    param = "params.range"
    runs_filtered = runs[
        (runs[param].isin([str(r) for r in ranges]))
        & (runs["params.preserve_overlap"] == str(preserve_overlap))
        & (runs["params.downsampling"].apply(lambda ds: resolve_downsampling(ds, downsampling)))
        ]
    with st.expander(f"Step={step} | PO={preserve_overlap} | downsampling={downsampling} -> *{len(runs_filtered)} Runs*"):
        st.write(f"Step = **{step}** | "
                 f"Preserve Overlap = **{preserve_overlap}** | "
                 f"Downsamplng = **{downsampling}**")
        if runs_filtered.shape[0] == 0:
            continue
        # st.dataframe(runs_filtered)
        key += 1
        basic_box_plot(runs_filtered, param=param, x_order=sorted(runs_filtered[param].unique().tolist()),
                       chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig)
