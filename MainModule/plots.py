import pandas as pd
import mlflow
import seaborn as sns
import matplotlib.pyplot as plt

M_NAMES = ["precision", "recall", "fscore"]
M_LABELS = ["Precision", "Recall", "F1-Score"]
MAP_AGE_RANGE = {
        "[20, 39]": "0-39",
        "[40, 59]": "40-59",
        "[60, 1000]": ">60"
    }


def main():
    # plot_distributions()
    random()
    # rest()


def rest():
    runs_rest = mlflow.search_runs(experiment_ids=["337062275817683573", "331509985598984853"])
    # AGE
    runs_age = runs_rest[runs_rest["params.subset_selection"] == "AGE"]
    runs_age.groupby(["params.range"]).size()
    runs_age["params.range"] = runs_age["params.range"].apply(lambda v: MAP_AGE_RANGE.get(v, v))
    runs_age = runs_age.sort_values(by="params.range")
    compare_metrics(runs_age, "range", "Age Range", "Age Subsets")
    # PLZ
    runs_plz = runs_rest[runs_rest["params.subset_selection"] == "PLZ"]
    compare_metrics(runs_plz, "equals", "", "PLZ, first two digits")
    # GENDER
    runs_gender = runs_rest[(runs_rest["params.subset_selection"] == "ATTRIBUTE_VALUE")
                            & (runs_rest["params.column"] == "GENDER")]
    compare_metrics(runs_gender, "equals", "Gender", "Gender")


def plot_distributions():
    filepath = "data/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv"
    col_names = "sourceID,globalID,localID,FIRSTNAME,MIDDLENAME,LASTNAME,YEAROFBIRTH,PLACEOFBIRTH,COUNTRY,CITY,PLZ,STREET,GENDER,ETHNIC,RACE".split(
        ",")
    df = pd.read_csv(filepath, names=col_names)

    df["YEAROFBIRTH"].hist(figsize=[13, 7])
    plt.suptitle("Year of Birth Distribution")
    plt.show()

    df["YEAROFBIRTH"].hist(bins=100, figsize=[13, 7])
    plt.suptitle("Year of Birth Distribution")
    plt.show()

    gender_dist = df.groupby(["GENDER"]).size()
    plt.pie(x=[v for _, v in gender_dist.items()], labels=[k for k, _ in gender_dist.items()], autopct='%1.1f%%')
    plt.title("Gender Distribution")
    plt.show()

    df.RACE.map(lambda v: "O" if v in ["I", "A", "M", "P"] else v).value_counts().plot.bar(title="Race Distribution")
    plt.show()

    df.PLZ.map(lambda plz: str(plz)[:2]).value_counts().plot.bar(title="Zip Code Distribution (first two digits)")
    plt.show()

    df.PLZ.map(lambda plz: str(plz)[:3]).value_counts().plot.bar(title="Zip Code Distribution (first three digits)")
    plt.show()


def random():
    runs_random = mlflow.search_runs(experiment_ids=["651112999057083515"])
    runs_random = runs_random.apply(pd.to_numeric, errors="ignore")
    mean_color_maps(runs_random)
    influence_of_size(runs_random)
    influence_of_overlap(runs_random)


def compare_metrics(data, param, param_name, title):
    plt.rcParams['figure.figsize'] = [15, 5]
    fig, ax = plt.subplots(1, 3, sharey=True)
    sns.boxplot(data=data, x=f"params.{param}", y="metrics.precision", ax=ax[0])
    sns.boxplot(data=data, x=f"params.{param}", y="metrics.recall", ax=ax[1])
    sns.boxplot(data=data, x=f"params.{param}", y="metrics.fscore", ax=ax[2])
    ax[0].set_ylabel("Precision")
    ax[1].set_ylabel("Recall")
    ax[2].set_ylabel("F1-Score")
    for x in ax.flatten():
        x.xaxis.set_tick_params(labelbottom=True)
        x.yaxis.set_tick_params(labelleft=True)
    [x.set_xlabel(param_name) for x in ax]
    fig.suptitle(title)
    fig.show()


def mean_color_maps(runs):
    runs_mean = runs.groupby(by=["params.overlap", "params.size"]).mean()
    for m, t in zip(M_NAMES, M_LABELS):
        colormap(m, t, runs_mean)


def colormap(metric, title, runs):
    cmap = "cool"
    fig, ax = plt.subplots()
    _metric = runs[f"metrics.{metric}"]
    g = sns.scatterplot(x="params.size", y="params.overlap", data=runs, c=_metric, cmap=cmap, size=_metric,
                        legend=False)
    g.set_xlabel("Subset Size")
    g.set_ylabel("Overlap")

    # color bar
    norm = plt.Normalize(_metric.min(), _metric.max())
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cax = fig.add_axes([ax.get_position().x1 + 0.05, ax.get_position().y0, 0.06, ax.get_position().height / 2])
    ax.figure.colorbar(sm, cax=cax)

    plt.suptitle(title)
    plt.show()


def influence_of_size(runs, overlap=.2):
    influence_of_a(runs, a_name="size", a_label="Subset Size", b_name="overlap", b_val=overlap, ylim=[.5, 1])


def influence_of_overlap(runs, size=100_000):
    influence_of_a(runs, a_name="overlap", a_label="Overlap", b_name="size", b_val=size, ylim=[.5, 1])


def influence_of_a(runs, a_name, a_label, b_name, b_val, ylim=None):
    runs_fixed_b = runs[runs[f"params.{b_name}"] == b_val]
    for m in zip(M_NAMES, M_LABELS):
        box_plot(data=runs_fixed_b, param=(a_name, a_label), metric=m, ylim=ylim)


def box_plot(data, param: (str, str), metric: (str, str), ylim=None):
    m_name, m_label = metric
    p_name, p_label = param
    sns.boxplot(data=data, x=f"params.{p_name}", y=f"metrics.{m_name}")
    plt.xlabel(p_label)
    plt.ylabel(m_label)
    if ylim is not None:
        plt.ylim(ylim)
    plt.show()


if __name__ == "__main__":
    main()
