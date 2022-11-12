from subset_generator import SubsetGenerator


def main():
    filepath = "data/2021_NCVR_Panse_001/dataset_ncvr_dirty.csv"
    col_names = "sourceID,globalID,localID,FIRSTNAME,MIDDLENAME,LASTNAME,YEAROFBIRTH,PLACEOFBIRTH,COUNTRY,CITY,PLZ,STREET,GENDER,ETHNIC,RACE".split(
        ",")
    sg = SubsetGenerator(filepath, col_names)
    random_sample = sg.random_sample(size=300, seed=1, overlap=0.1)
    random_sample.to_csv("data/out/random_300_1_0.1.csv", index=False, header=False)


if __name__ == "__main__":
    main()
