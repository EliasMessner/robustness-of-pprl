from streamlit_entry import *

key = 0

st.set_page_config(page_title="Attribute Value Length")
st.write("# Attribute Value Length")

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


exp_ids = experiment_multiselect(default=["841125091019294509"])
runs = get_runs(exp_ids)
assert (runs["params.subset_selection"] == "ATTRIBUTE_VALUE").all()
assert (~runs["params.length"].isnull()).all()

col_options = runs["params.column"].unique().tolist()
col_selections = st.multiselect("Column", options=col_options, default=col_options)

for col in col_selections:
    param = "params.length"
    runs_filtered = runs[runs["params.column"] == col]
    with st.expander(f"Column = **{col}**"):
        if runs_filtered.shape[0] == 0:
            continue
        key += 1
        basic_box_plot(runs_filtered, param=param, x_order=sorted(runs_filtered[param].unique().tolist()),
                       chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig)
