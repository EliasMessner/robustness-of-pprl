package PPRL.src;

public record Parameters(LinkingMode linkingMode, HashingMode hashingMode, String h1, String h2, boolean blocking, boolean weightedAttributes, String tokenSalting, int l, int k, double t) {
}
