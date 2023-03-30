import itertools
from streamlit_entry import *

key = 0

st.set_page_config(page_title="Error Rate", layout="wide")
st.write("# Error Rate")

CHART_ARRANGEMENT = st.sidebar.radio(label="Chart Arrangement", options=["Horizontal", "Vertical"])
st.sidebar.write("Figure Size")
key += 1
size_x = st.sidebar.slider(label="X", min_value=2, max_value=20, value=DEFAULT_FIG_SIZE[0], key=key)
key += 1
size_y = st.sidebar.slider(label="Y", min_value=2, max_value=15, value=DEFAULT_FIG_SIZE[1], key=key)
fig = plt.figure(figsize=(size_x, size_y))


def experiment_multiselect(default):
    options = [f"{e.experiment_id} '{e.name}'" for e in mlflow.search_experiments()]  # show ID and name
    default = [f"{_id} '{mlflow.get_experiment(_id).name}'" for _id in default]
    selection = st.multiselect("Experiments", options, default)
    return [value.split()[0] for value in selection]  # return only ID

default_exp = mlflow.search_experiments(filter_string="name ILIKE '%ERROR-RATE%'")
default_exp = [sorted(default_exp, key=lambda e: e.creation_time, reverse=True)[0]]  # if many are found, use the latest
exp_ids = experiment_multiselect(default=[e.experiment_id for e in default_exp])
runs = get_runs(exp_ids)
assert (runs["params.subset_selection"] == "ERROR_RATE").all()

with st.expander(f"Found {runs.shape[0]} runs"):
    st.write(runs)

step_selections = st.multiselect("Step", options=[2, 3], default=[2, 3])
preserve_overlap_selections = st.multiselect("Preserve Overlap", options=["True", "False"], default=["True", "False"])
downsampling_selections = st.multiselect("Downsampling", options=DOWNSAMPLING_OPTIONS, default=["Exact Value"])


def resolve_downsampling(observed_value, selected_value):
    if selected_value == "Exact Value":
        return str(observed_value).isdigit()
    return str(observed_value) == str(selected_value)


for step, preserve_overlap, downsampling in itertools.product(step_selections, preserve_overlap_selections,
                                                              downsampling_selections):
    if step == 2:
        ranges = [[1, 2], [3, 4], [5, 6], [7, 8], [9, 12]]
    elif step == 3:
        ranges = [[1, 3], [4, 6], [7, 10]]
    else:
        raise ValueError()
    param = "params.range"
    #st.write(runs)
    runs_filtered = runs[runs.apply(lambda row: str(row[param]) in [str(r) for r in ranges]
                                    and str(row["params.preserve_overlap"]) == str(preserve_overlap)
                                    and resolve_downsampling(row["params.downsampling"], downsampling), axis=1)]

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
