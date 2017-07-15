package com.thunken.sandow.spliterator;

import java.util.Spliterator;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import lombok.Builder;
import lombok.NonNull;

/**
 * Specialized implementation of {@code Spliterator} that uses pagination with
 * {@code from} and {@code size} to retrieve potentially large numbers of
 * results from a single search request in an Elasticsearch index.
 *
 * <p>
 * See <a href=
 * "https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html">Elasticsearch
 * Reference: From / Size</a> for more information.
 *
 * @see SearchHit
 * @see Spliterator
 */
public class PaginatingSearchHitSpliterator extends SearchHitSpliterator {

	private int from;

	@NonNull
	private final SearchRequestBuilder searchRequest;

	@Builder.Default
	private int size = 10;

	@Builder
	private PaginatingSearchHitSpliterator(@NonNull final SearchRequestBuilder searchRequest, final int from,
			final int size) {
		super(searchRequest.setFrom(from).setSize(size).execute());
		this.from = from;
		this.size = size;
		this.searchRequest = searchRequest;
	}

	@Override
	protected ListenableActionFuture<SearchResponse> getNextBatch(@NonNull final SearchResponse searchResponse) {
		return searchRequest.setFrom(from += getCursor()).setSize(size).execute();
	}

}
