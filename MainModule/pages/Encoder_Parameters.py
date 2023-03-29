from streamlit_entry import *

key = 0

st.set_page_config(page_title="Encoder Parameters", layout="wide")
st.write("# Encoder Parameters")

CHART_ARRANGEMENT = st.sidebar.radio(label="Chart Arrangement", options=["Horizontal", "Vertical"])
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


@st.cache_data
def get_seeds(_seed_count, _runs):
    return _runs["tags.seed"].unique().tolist()[:_seed_count]


@st.cache_data
def get_max_seed_count(_runs):
    return _runs["tags.seed"].unique().shape[0]


default_exp = mlflow.search_experiments(filter_string="name ILIKE '%ENCODER-PARAMS%'")
default_exp = [sorted(default_exp, key=lambda e: e.creation_time, reverse=True)[0]]  # if many are found, use the latest
exp_ids = experiment_multiselect(default=[e.experiment_id for e in default_exp])
runs = get_runs(exp_ids)

max_seed_count = get_max_seed_count(runs)
if max_seed_count > 1:
    seed_count = st.slider("Seed Count", 1, max_seed_count, value=max_seed_count)
else:
    seed_count = 1
seeds = runs["tags.seed"].unique().tolist()[:seed_count]


st.write(f"## Influence of Seed")
param = "tags.seed"
k_options = sorted(runs["tags.k"].unique().tolist())
key += 1
select_all = st.checkbox("Select All", key=key)
key += 1
k_selections = st.multiselect("k (# hash functions)", options=k_options, default=[] if not select_all else k_options,
                              key=key)

for k in k_selections:
    runs_filtered = runs[
        (runs["tags.k"] == k)
        & runs["tags.seed"].isin(seeds)
        ]
    with st.expander(f"k = {k}"):
        if runs_filtered.shape[0] == 0:
            continue
        key += 1
        basic_box_plot(runs_filtered, param=param, x_order=sorted(runs_filtered[param].unique().tolist()),
                       chart_arrangement=CHART_ARRANGEMENT, fig=fig, key=key)


st.write(f"## Influence of k (# hash functions)")
param = "tags.k"
t_options = sorted(runs["tags.t"].unique().tolist())
key += 1
select_all = st.checkbox("Select All", key=key)
key += 1
t_selections = st.multiselect("t (threshold)", options=t_options, default=[] if not select_all else t_options,
                              key=key)

for t in t_selections:
    runs_filtered = runs[
        (runs["tags.t"] == t)
        & runs["tags.seed"].isin(seeds)
        ]
    with st.expander(f"t = {t}"):
        if runs_filtered.shape[0] == 0:
            continue
        key += 1
        basic_box_plot(runs_filtered, param=param, x_order=sorted(runs_filtered[param].unique().tolist()),
                       chart_arrangement=CHART_ARRANGEMENT, fig=fig, key=key)
