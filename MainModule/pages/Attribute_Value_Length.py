from streamlit_entry import *
from ast import literal_eval

key = 0

st.set_page_config(page_title="Attribute Value Length", layout="wide")
st.write("# Attribute Value Length")

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

default_exp = mlflow.search_experiments(filter_string="name ILIKE '%ATTR-VAL-LENGTH%'")
default_exp = [sorted(default_exp, key=lambda e: e.creation_time, reverse=True)[0]]  # if many are found, use the latest
exp_ids = experiment_multiselect(default=[e.experiment_id for e in default_exp])
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
        basic_box_plot(runs_filtered, param=param, x_order=sorted(runs_filtered[param].unique().tolist(),
                                                                  key=lambda el: literal_eval(el)[0]),
                       chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig)
