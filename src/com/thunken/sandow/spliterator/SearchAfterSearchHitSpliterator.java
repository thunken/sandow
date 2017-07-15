package com.thunken.sandow.spliterator;

import java.util.Spliterator;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import lombok.Builder;
import lombok.NonNull;

/**
 *
 * Specialized implementation of {@code Spliterator} that uses
 * {@code search_after} to retrieve potentially large numbers of results from a
 * single search request in an Elasticsearch index.
 *
 * <p>
 * See <a href=
 * "https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-after.html">Elasticsearch
 * Reference: Search After</a> for more information.
 *
 * @see SearchHit
 * @see Spliterator
 */
public class SearchAfterSearchHitSpliterator extends SearchHitSpliterator {

	@Builder.Default
	private boolean addTieBreaker = true;

	private final SearchRequestBuilder searchRequest;

	@Builder.Default
	private int size = 10;

	private Object[] sortValues;

	@Builder
	private SearchAfterSearchHitSpliterator(@NonNull final SearchRequestBuilder searchRequest, final int size,
			final boolean addTieBreaker) {
		super((addTieBreaker ? searchRequest.addSort("_uid", SortOrder.ASC) : searchRequest).execute());
		this.searchRequest = searchRequest;
		this.size = size;
	}

	@Override
	protected void update(@NonNull final SearchHit searchHit) {
		sortValues = searchHit.getSortValues();
	}

	@Override
	protected ListenableActionFuture<SearchResponse> getNextBatch(@NonNull final SearchResponse searchResponse) {
		return (sortValues == null ? searchRequest : searchRequest.searchAfter(sortValues)).setSize(size).execute();
	}

}
