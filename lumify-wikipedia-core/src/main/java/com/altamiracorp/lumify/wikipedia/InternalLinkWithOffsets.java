package com.altamiracorp.lumify.wikipedia;

import org.sweble.wikitext.lazy.parser.InternalLink;

public class InternalLinkWithOffsets {
    private final InternalLink link;
    private final int startOffset;
    private final int endOffset;

    public InternalLinkWithOffsets(InternalLink link, int startOffset, int endOffset) {
        this.link = link;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public InternalLink getLink() {
        return link;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }
}
