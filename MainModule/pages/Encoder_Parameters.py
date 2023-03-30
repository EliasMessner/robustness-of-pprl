from streamlit_entry import *

key = 0

st.set_page_config(page_title="Encoder Parameters", layout="wide")
st.write("# Encoder Parameters")

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


@st.cache_data
def get_seeds(_seed_count, _runs):
    return _runs["tags.seed"].unique().tolist()[:_seed_count]


@st.cache_data
def get_max_seed_count(_runs):
    return _runs["tags.seed"].unique().shape[0]


st.write(f"## Influence of k (# hash functions)")

default_exp = mlflow.search_experiments(filter_string="name ILIKE '%ENCODER-PARAMS-k%'")
default_exp = [sorted(default_exp, key=lambda e: e.creation_time, reverse=True)[0]]  # if many are found, use the latest
exp_ids = experiment_multiselect(default=[e.experiment_id for e in default_exp])
runs_k = get_runs(exp_ids)

max_seed_count = get_max_seed_count(runs_k)
if max_seed_count > 1:
    seed_count = st.slider("Seed Count", 1, max_seed_count, value=max_seed_count)
else:
    seed_count = 1
seeds = runs_k["tags.seed"].unique().tolist()[:seed_count]


runs_k_filtered = runs_k[runs_k["tags.seed"].isin(seeds)]
param = "tags.k"
key += 1
basic_box_plot(runs_k_filtered, param=param, x_order=sorted(runs_k_filtered[param].unique().tolist()),
               chart_arrangement=CHART_ARRANGEMENT, fig=fig, key=key)


st.write(f"## Influence of Seed")

default_exp = mlflow.search_experiments(filter_string="name ILIKE '%ENCODER-PARAMS-seed%'")
default_exp = [sorted(default_exp, key=lambda e: e.creation_time, reverse=True)[0]]  # if many are found, use the latest
exp_ids = experiment_multiselect(default=[e.experiment_id for e in default_exp])
runs_s = get_runs(exp_ids)

param = "tags.seed"
key += 1
basic_box_plot(runs_s, param=param, x_order=sorted(runs_s[param].unique().tolist()),
               chart_arrangement=CHART_ARRANGEMENT, fig=fig, key=key)
