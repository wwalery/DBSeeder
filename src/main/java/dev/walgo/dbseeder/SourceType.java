package dev.walgo.dbseeder;

public enum SourceType {
    CSV(".csv");

    private final String extension;

    SourceType(String extension) {
        this.extension = extension;
    }

    public String getExtension() {
        return extension;
    }

}
