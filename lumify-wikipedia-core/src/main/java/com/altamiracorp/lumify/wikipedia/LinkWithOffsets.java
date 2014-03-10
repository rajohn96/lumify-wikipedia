package com.altamiracorp.lumify.wikipedia;

public interface LinkWithOffsets {
    String getLinkTargetWithoutHash();

    int getStartOffset();

    int getEndOffset();
}
