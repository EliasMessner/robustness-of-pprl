from streamlit_entry import *

key = 0

st.set_page_config(page_title="Age", layout="wide")
st.write("# Age")
st.write("Analyze Influence of Year of Birth")

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


default_exp = mlflow.search_experiments(filter_string="name ILIKE '%AGE%'")
default_exp = [sorted(default_exp, key=lambda e: e.creation_time, reverse=True)[0]]  # if many are found, use the latest
exp_ids = experiment_multiselect(default=[e.experiment_id for e in default_exp])
runs = get_runs(exp_ids)

param = "params.range"
key += 1
basic_box_plot(runs, param=param, x_order=sorted(runs[param].unique().tolist()),
               chart_arrangement=CHART_ARRANGEMENT, fig=fig, key=key)
