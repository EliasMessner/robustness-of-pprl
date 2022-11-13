package PPRL;

public enum LinkingMode {
    STABLE_MARRIAGE,
    SEMI_MONOGAMOUS_LEFT,
    SEMI_MONOGAMOUS_RIGHT,
    POLYGAMOUS;

    public static LinkingMode parseFromString(String s) {
        return switch (s.toUpperCase()) {
            case "SM" -> LinkingMode.STABLE_MARRIAGE;
            case "SL", "SEMI_LEFT" -> LinkingMode.SEMI_MONOGAMOUS_LEFT;
            case "SR", "SEMI_RIGHT" -> LinkingMode.SEMI_MONOGAMOUS_RIGHT;
            case "PO" -> LinkingMode.POLYGAMOUS;
            default -> throw new IllegalArgumentException("Unexpected Value for Linking Mode '" + s + "'");
        };
    }
}
