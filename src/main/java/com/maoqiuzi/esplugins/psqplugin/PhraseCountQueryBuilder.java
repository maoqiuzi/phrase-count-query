package com.maoqiuzi.esplugins.psqplugin;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.search.MatchQuery;

import java.io.IOException;

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

//    protected Query doToQuery(QueryShardContext context) throws IOException {
//
//        Analyzer analyzer = context.getMapperService().getIndexAnalyzers().get(this.analyzer);
//        if (analyzer == null) {
//            throw new IllegalArgumentException("No analyzer found for [" + this.analyzer + "]");
//        }
//        // validate context specific fields
//        if (analyzer != null && context.getIndexAnalyzers().get(this.analyzer) == null) {
//            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + this.analyzer + "] not found");
//        }
//
//
//        PhraseCountQuery.PCQBuilder PCQBuilder = new PhraseCountQuery.PCQBuilder();
//        PCQBuilder.setSlop(slop);
//        try (TokenStream source = analyzer.tokenStream(fieldName, value.toString())) {
//            CachingTokenFilter stream = new CachingTokenFilter(source);
//            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
//            PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
//            int position = -1;
//            stream.reset();
//            while (stream.incrementToken()) {
//                position += 1;
//                PCQBuilder.add(new Term(fieldName, termAtt.getBytesRef()), position);
//            }
//
//            return PCQBuilder.build();
//        } catch (IOException e) {
//            throw new RuntimeException("Error analyzing query text", e);
//        }
//
//    }

}

