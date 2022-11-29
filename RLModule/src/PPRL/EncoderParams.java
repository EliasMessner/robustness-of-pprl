package PPRL;

public record EncoderParams(HashingMode hashingMode,
                            String h1, String h2,
                            boolean weightedAttributes,
                            String tokenSalting, int l, int k) {
}
