package PPRL;

public enum HashingMode {
    DOUBLE_HASHING,
    ENHANCED_DOUBLE_HASHING,
    TRIPLE_HASHING,
    RANDOM_HASHING;

    public static HashingMode parseFromString(String s) {
        return switch (s.toUpperCase()) {
            case "DH" -> HashingMode.DOUBLE_HASHING;
            case "ED" -> HashingMode.ENHANCED_DOUBLE_HASHING;
            case "TH" -> HashingMode.TRIPLE_HASHING;
            case "RH" -> HashingMode.RANDOM_HASHING;
            default -> throw new IllegalArgumentException("Unexpected Value for Hashing Mode '" + s + "'");
        };
    }
}
