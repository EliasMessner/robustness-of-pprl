from streamlit_entry import *

st.set_page_config(page_title="Attribute Value Length")
st.write("# Attribute Value Length")

CHART_ARRANGEMENT = st.sidebar.radio(label="Chart Arrangement", options=["Vertical", "Horizontal"])


def experiment_multiselect(default):
    options = [f"{e.experiment_id} '{e.name}'" for e in mlflow.search_experiments()]  # show ID and name
    default = [f"{_id} '{mlflow.get_experiment(_id).name}'" for _id in default]
    selection = st.multiselect("Experiments", options, default)
    return [value.split()[0] for value in selection]  # return only ID


exp_ids = experiment_multiselect(default=["470582673248318889"])
runs = get_runs(exp_ids)
assert (runs["params.subset_selection"] == "ATTRIBUTE_VALUE").all()
assert (~runs["params.length"].isnull()).all()

col_options = runs["params.column"].unique().tolist()
col_selections = st.multiselect("Column", options=col_options, default=col_options)

for col in col_selections:
    param = "params.length"
    runs_filtered = runs[runs["params.column"] == col]
    with st.expander(f"Column = {col}"):
        st.write(f"Column = **{col}**")
        if runs_filtered.shape[0] == 0:
            continue
        basic_box_plot(runs_filtered, param=param, x_order=sorted(runs_filtered[param].unique().tolist()),
                       chart_arrangement=CHART_ARRANGEMENT)
