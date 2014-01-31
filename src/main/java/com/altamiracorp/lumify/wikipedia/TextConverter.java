package com.altamiracorp.lumify.wikipedia;

import de.fau.cs.osr.ptk.common.AstVisitor;
import de.fau.cs.osr.ptk.common.ast.AstNode;
import de.fau.cs.osr.ptk.common.ast.NodeList;
import de.fau.cs.osr.ptk.common.ast.Text;
import de.fau.cs.osr.utils.StringUtils;
import org.sweble.wikitext.engine.Page;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.utils.EntityReferences;
import org.sweble.wikitext.engine.utils.SimpleWikiConfiguration;
import org.sweble.wikitext.lazy.LinkTargetException;
import org.sweble.wikitext.lazy.encval.IllegalCodePoint;
import org.sweble.wikitext.lazy.parser.*;
import org.sweble.wikitext.lazy.preprocessor.*;
import org.sweble.wikitext.lazy.utils.XmlCharRef;
import org.sweble.wikitext.lazy.utils.XmlEntityRef;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class TextConverter extends AstVisitor {
    private static final Pattern ws = Pattern.compile("\\s+");

    private final SimpleWikiConfiguration config;

    private StringBuilder sb;

    private StringBuilder line;

    private int extLinkNum;

    private boolean pastBod;

    private int needNewlines;

    private boolean needSpace;

    private LinkedList<Integer> sections;
    private List<InternalLinkWithOffsets> internalLinks = new ArrayList<InternalLinkWithOffsets>();

    // =========================================================================

    public TextConverter(SimpleWikiConfiguration config) {
        this.config = config;
    }

    @Override
    protected boolean before(AstNode node) {
        // This method is called by go() before visitation starts
        sb = new StringBuilder();
        line = new StringBuilder();
        extLinkNum = 1;
        pastBod = false;
        needNewlines = 0;
        needSpace = false;
        sections = new LinkedList<Integer>();
        return super.before(node);
    }

    @Override
    protected Object after(AstNode node, Object result) {
        finishLine();

        // This method is called by go() after visitation has finished
        // The return value will be passed to go() which passes it to the caller
        return sb.toString();
    }

    public void visit(AstNode n) {
    }

    public void visit(NodeList n) {
        iterate(n);
    }

    public void visit(Itemization e) {
        iterate(e.getContent());
    }

    public void visit(ItemizationItem i) {
        newline(1);
        iterate(i.getContent());
    }

    public void visit(Enumeration e) {
        iterate(e.getContent());
    }

    public void visit(EnumerationItem item) {
        newline(1);
        iterate(item.getContent());
    }

    public void visit(Page p) {
        iterate(p.getContent());
    }

    public void visit(Text text) {
        write(text.getContent());
    }

    public void visit(Whitespace w) {
        write(" ");
    }

    public void visit(Bold b) {
        iterate(b.getContent());
    }

    public void visit(Italics i) {
        iterate(i.getContent());
    }

    public void visit(XmlCharRef cr) {
        write(Character.toChars(cr.getCodePoint()));
    }

    public void visit(XmlEntityRef er) {
        String ch = EntityReferences.resolve(er.getName());
        if (ch == null) {
            write('&');
            write(er.getName());
            write(';');
        } else {
            write(ch);
        }
    }

    public void visit(Url url) {
        write(url.getProtocol());
        write(':');
        write(url.getPath());
    }

    public void visit(ExternalLink link) {
        write('[');
        write(extLinkNum++);
        write(']');
    }

    public void visit(InternalLink link) {
        int startOffset = getCurrentOffset();
        try {
            PageTitle page = PageTitle.make(config, link.getTarget());
            if (page.getNamespace().equals(config.getNamespace("Category"))) {
                return;
            }
        } catch (LinkTargetException e) {
        }

        write(link.getPrefix());
        if (link.getTitle().getContent() == null || link.getTitle().getContent().isEmpty()) {
            write(link.getTarget());
        } else {
            iterate(link.getTitle());
        }
        write(link.getPostfix());
        int endOffset = getCurrentOffset();

        internalLinks.add(new InternalLinkWithOffsets(link, startOffset, endOffset));
    }

    public void visit(Section s) {
        finishLine();
        StringBuilder saveSb = sb;

        sb = new StringBuilder();

        iterate(s.getTitle());
        finishLine();
        String title = sb.toString().trim();

        sb = saveSb;

        if (s.getLevel() >= 1) {
            while (sections.size() > s.getLevel()) {
                sections.removeLast();
            }
            while (sections.size() < s.getLevel()) {
                sections.add(1);
            }

            StringBuilder sb2 = new StringBuilder();
            for (int i = 0; i < sections.size(); ++i) {
                if (i < 1) {
                    continue;
                }

                sb2.append(sections.get(i));
                sb2.append('.');
            }

            if (sb2.length() > 0) {
                sb2.append(' ');
            }
            sb2.append(title);
            title = sb2.toString();
        }

        newline(2);
        write(title);
        newline(2);

        iterate(s.getBody());

        while (sections.size() > s.getLevel())
            sections.removeLast();
        sections.add(sections.removeLast() + 1);
    }

    public void visit(Paragraph p) {
        iterate(p.getContent());
        newline(2);
    }

    public void visit(HorizontalRule hr) {
        newline(1);
    }

    public void visit(XmlElement e) {
        if (e.getName().equalsIgnoreCase("br")) {
            newline(1);
        } else {
            iterate(e.getBody());
        }
    }

    // =========================================================================
    // Stuff we want to hide

    public void visit(ImageLink n) {
    }

    public void visit(IllegalCodePoint n) {
    }

    public void visit(XmlComment n) {
    }

    public void visit(Template n) {
    }

    public void visit(TemplateArgument n) {
    }

    public void visit(TemplateParameter n) {
    }

    public void visit(TagExtension n) {
    }

    public void visit(MagicWord n) {
    }

    // =========================================================================

    private void newline(int num) {
        if (pastBod) {
            if (num > needNewlines) {
                needNewlines = num;
            }
        }
    }

    private void wantSpace() {
        if (pastBod) {
            needSpace = true;
        }
    }

    private void finishLine() {
        sb.append(line.toString());
        line.setLength(0);
    }

    private void writeNewlines(int num) {
        finishLine();
        sb.append(StringUtils.strrep('\n', num));
        needNewlines = 0;
        needSpace = false;
    }

    private void writeWord(String s) {
        int length = s.length();
        if (length == 0)
            return;

        if (needSpace && needNewlines <= 0) {
            line.append(' ');
        }

        if (needNewlines > 0) {
            writeNewlines(needNewlines);
        }

        needSpace = false;
        pastBod = true;
        line.append(s);
    }

    private void write(String s) {
        if (s.isEmpty())
            return;

        if (Character.isSpaceChar(s.charAt(0))) {
            wantSpace();
        }

        String[] words = ws.split(s);
        for (int i = 0; i < words.length; ) {
            writeWord(words[i]);
            if (++i < words.length) {
                wantSpace();
            }
        }

        if (Character.isSpaceChar(s.charAt(s.length() - 1))) {
            wantSpace();
        }
    }

    private void write(char[] cs) {
        write(String.valueOf(cs));
    }

    private void write(char ch) {
        writeWord(String.valueOf(ch));
    }

    private void write(int num) {
        writeWord(String.valueOf(num));
    }

    public List<InternalLinkWithOffsets> getInternalLinks() {
        return internalLinks;
    }

    public int getCurrentOffset() {
        return sb.length() + line.length();
    }
}