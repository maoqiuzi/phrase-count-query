package com.maoqiuzi.esplugins.psqplugin;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;
import java.util.Optional;

/**
 * Created by maoqiuzi on 5/23/17.
 */
public class PhraseCountQueryBuilder extends MatchPhraseQueryBuilder implements QueryBuilder {
    private String analyzer;
    private int slop = MatchQuery.DEFAULT_PHRASE_SLOP;
    private final String fieldName;

    private final Object value;

    public static final String NAME = "phrase_count_query";

    public PhraseCountQueryBuilder(String fieldName, Object value) {
        super(fieldName, value);
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
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
        slop = in.readVInt();
        analyzer = in.readOptionalString();
    }

    protected Query doToQuery(QueryShardContext context) throws IOException {
// validate context specific fields
//        if (analyzer != null && context.getIndexAnalyzers().get(analyzer) == null) {
//            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + analyzer + "] not found");
//        }
//
//        MatchQuery matchQuery = new MatchQuery(context);
//        matchQuery.setAnalyzer(analyzer);
//        matchQuery.setPhraseSlop(slop);
//
//        return matchQuery.parse(MatchQuery.Type.PHRASE, fieldName, value);
        Analyzer analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
        if (analyzer == null) {
            throw new IllegalArgumentException("No analyzer found for [" + this.analyzer + "]");
        }
        // validate context specific fields
        if (analyzer != null && context.getIndexAnalyzers().get(this.analyzer) == null) {
            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + this.analyzer + "] not found");
        }


        PhraseCountQuery.PCQBuilder PCQBuilder = new PhraseCountQuery.PCQBuilder();
        PCQBuilder.setSlop(slop);
        try (TokenStream source = analyzer.tokenStream(fieldName, value.toString())) {
            CachingTokenFilter stream = new CachingTokenFilter(source);
            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
            int position = -1;
            stream.reset();
            while (stream.incrementToken()) {
                position += 1;
                PCQBuilder.add(new Term(fieldName, termAtt.getBytesRef()), position);
            }
            return PCQBuilder.build();
        } catch (IOException e) {
            throw new RuntimeException("Error analyzing query text", e);
        }

    }

    public static Optional<MatchPhraseQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
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

