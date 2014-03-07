package com.altamiracorp.lumify.wikipedia;

import org.sweble.wikitext.lazy.preprocessor.Redirect;

public class RedirectWithOffsets implements LinkWithOffsets {
    private final Redirect redirect;
    private final int startOffset;
    private final int endOffset;

    public RedirectWithOffsets(Redirect redirect, int startOffset, int endOffset) {
        this.redirect = redirect;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public Redirect getRedirect() {
        return redirect;
    }

    @Override
    public String getLinkTargetWithoutHash() {
        String target = getRedirect().getTarget();
        if (target == null) {
            return null;
        }

        int hashIndex = target.indexOf('#');
        if (hashIndex > 0) {
            target = target.substring(0, hashIndex);
        }

        return target;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }
}
