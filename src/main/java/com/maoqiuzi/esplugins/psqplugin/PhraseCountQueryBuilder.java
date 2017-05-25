package com.maoqiuzi.esplugins.psqplugin;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Created by maoqiuzi on 5/23/17.
 */
public class PhraseCountQueryBuilder extends AbstractQueryBuilder<PhraseCountQueryBuilder> {
    private String analyzer;
    private int slop = MatchQuery.DEFAULT_PHRASE_SLOP;
    private final String fieldName;
    public static final ParseField SLOP_FIELD = new ParseField("slop", "phrase_slop");


    private final Object value;

    public static final String NAME = "phrase_count_query";

    public PhraseCountQueryBuilder(String fieldName, Object value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    public PhraseCountQueryBuilder(StreamInput in) throws IOException {
        fieldName = in.readString();
        value = in.readGenericValue();
        slop = in.readVInt();
        analyzer = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        out.writeVInt(slop);
        out.writeOptionalString(analyzer);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);

        builder.field(MatchQueryBuilder.QUERY_FIELD.getPreferredName(), value);
        if (analyzer != null) {
            builder.field(MatchQueryBuilder.ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(SLOP_FIELD.getPreferredName(), slop);
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected boolean doEquals(PhraseCountQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(value, other.value) && Objects.equals(analyzer, other.analyzer)
                && Objects.equals(slop, other.slop);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, analyzer, slop);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public PhraseCountQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Get the analyzer to use, if previously set, otherwise <tt>null</tt> */
    public String analyzer() {
        return this.analyzer;
    }

    /** Sets a slop factor for phrase queries */
    public PhraseCountQueryBuilder slop(int slop) {
        if (slop < 0) {
            throw new IllegalArgumentException("No negative slop allowed.");
        }
        this.slop = slop;
        return this;
    }

    /** Get the slop factor for phrase queries. */
    public int slop() {
        return this.slop;
    }

    protected Query doToQuery(QueryShardContext context) throws IOException {
        Analyzer analyzer = context.getMapperService().searchAnalyzer();

        try (TokenStream source = analyzer.tokenStream(fieldName, value.toString())) {
            CachingTokenFilter stream = new CachingTokenFilter(source);
            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            if (termAtt == null) {
                return null;
            }
            List<Term> terms = new ArrayList<>();
            stream.reset();
            while (stream.incrementToken()) {
                terms.add(new Term(fieldName, termAtt.getBytesRef()));
            }
            return new PhraseCountQuery(slop, terms.toArray(new Term[terms.size()]));
        } catch (IOException e) {
            throw new RuntimeException("Error analyzing query text", e);
        }

    }
    //XSON (maps to Content-Type application/xson) is an optimized binary representation of JSON.
    public static Optional<PhraseCountQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        int slop = MatchQuery.DEFAULT_PHRASE_SLOP;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (MatchQueryBuilder.QUERY_FIELD.match(currentFieldName)) {
                            value = parser.objectText();
                        } else if (MatchQueryBuilder.ANALYZER_FIELD.match(currentFieldName)) {
                            analyzer = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                            boost = parser.floatValue();
                        } else if (SLOP_FIELD.match(currentFieldName)) {
                            slop = parser.intValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = parser.objectText();
            }
        }

        PhraseCountQueryBuilder matchQuery = new PhraseCountQueryBuilder(fieldName, value);
        matchQuery.analyzer(analyzer);
        matchQuery.slop(slop);
        matchQuery.queryName(queryName);
        matchQuery.boost(boost);
        return Optional.of(matchQuery);
    }

}

