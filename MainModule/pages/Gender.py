from streamlit_entry import *

st.set_page_config(page_title="Gender")
st.write("# Gender")

CHART_ARRANGEMENT = st.sidebar.radio(label="Chart Arrangement", options=["Vertical", "Horizontal"])

exp_ids = experiment_multiselect(default=["535118992061034788"])
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
    basic_box_plot(runs_exact_val, param=param, chart_arrangement=CHART_ARRANGEMENT)

with st.expander("Gender Distribution"):
    st.write("## Gender Distribution")
    param = "params.dist"
    runs_dist = runs[~runs[param].isnull()]
    basic_box_plot(runs_dist, param=param, x_order=sorted(runs_dist[param].unique().tolist()),
                   chart_arrangement=CHART_ARRANGEMENT)
