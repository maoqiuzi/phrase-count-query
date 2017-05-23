package com.maoqiuzi.esplugins.psqplugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

/**
 * Created by maoqiuzi on 5/23/17.
 */
public class PhraseCountQueryBuilder extends MatchPhraseQueryBuilder implements QueryBuilder {

    public PhraseCountQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }
}
