# example:
# java -cp .:lib/*:classes RLInterface.Main -d "../data/dataset_variations/random_300_1_0.1.csv" -o "../data/matchings/random_300_1_0.1.csv" -l 1000 -k 10 -t 0.75

# with arguments passed on script execution:
java -cp .:lib/*:classes RLInterface.Main $*