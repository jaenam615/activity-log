package com.example.activitylog.recovery;

public enum JobState {
    STARTED("started"),
    SUCCESS("success"),
    FAILED("failed"),
    RECOVERED("recovered");

    private final String dirName;

    JobState(String dirName) {
        this.dirName = dirName;
    }

    public String dirName() {
        return dirName;
    }
}
