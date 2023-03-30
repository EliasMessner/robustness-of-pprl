from streamlit_entry import *

key = 0


st.set_page_config(page_title="Gender", layout="wide")
st.write("# Gender")

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


default_exp = mlflow.search_experiments(filter_string="name ILIKE '%GENDER%'")
default_exp = [sorted(default_exp, key=lambda e: e.creation_time, reverse=True)[0]]  # if many are found, use the latest
exp_ids = experiment_multiselect(default=[e.experiment_id for e in default_exp])
runs = get_runs(exp_ids)

assert (runs["params.subset_selection"] == "ATTRIBUTE_VALUE").all()
assert (runs["params.column"] == "GENDER").all()

with st.expander(f"Found {runs.shape[0]} runs:"):
    st.dataframe(runs)

# with st.expander("Exact Value"):
#     #runs = runs.dropna(subset="params.size")
#     st.write("## Exact Value")
#     st.write("### Including 'U'")
#     param = "params.equals"
#     runs_exact_val_with_u = runs[runs.apply(lambda row: row[param] is not None and row["params.size"] == 60965, axis=1)]
#     st.write(runs["params.size"].value_counts())
#     st.write(f"**{len(runs_exact_val_with_u)}** Runs | sizes from "
#              f"**{int(runs_exact_val_with_u['params.size'].min())}** to **{int(runs_exact_val_with_u['params.size'].max())}**")
#     key += 1
#     basic_box_plot(runs_exact_val_with_u, param=param, chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig, x_label="gender")
#
#     st.write("### Without 'U'")
#     runs_exact_val_without_u = runs[runs.apply(lambda row: row[param] is not None and row["params.size"] == 200000, axis=1)]
#     st.write(f"**{len(runs_exact_val_without_u)}** Runs | sizes from "
#              f"**{int(runs_exact_val_without_u['params.size'].min())}** to **{int(runs_exact_val_without_u['params.size'].max())}**")
#     key += 1
#     basic_box_plot(runs_exact_val_without_u, param=param, chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig,
#                    x_label="gender")

with st.expander("Gender Distribution"):
    st.write("## Gender Distribution")
    param = "params.dist"
    runs_dist = runs[~runs[param].isnull()]
    key += 1
    basic_box_plot(runs_dist, param=param, x_order=sorted(runs_dist[param].unique().tolist()),
                   chart_arrangement=CHART_ARRANGEMENT, key=key, fig=fig, x_label="gender distribution", rotate_xticks=45)
