package com.altamiracorp.lumify.wikipedia;

import org.sweble.wikitext.lazy.parser.InternalLink;

public class InternalLinkWithOffsets implements LinkWithOffsets {
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

    public String getLinkTargetWithoutHash() {
        String target = getLink().getTarget();
        if (target == null) {
            return null;
        }

        int hashIndex = target.indexOf('#');
        if (hashIndex > 0) {
            target = target.substring(0, hashIndex);
        }

        return target;
    }
}
