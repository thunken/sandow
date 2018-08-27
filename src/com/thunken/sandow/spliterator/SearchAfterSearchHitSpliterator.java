package com.thunken.sandow.spliterator;

import java.util.Spliterator;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import lombok.Builder;
import lombok.NonNull;

/**
 *
 * Specialized implementation of {@code Spliterator} that uses {@code search_after} to retrieve potentially large
 * numbers of results from a single search request in an Elasticsearch index.
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

	private final SearchRequestBuilder searchRequest;

	private final int size;

	private Object[] sortValues;

	@Builder
	private SearchAfterSearchHitSpliterator(@NonNull final SearchRequestBuilder searchRequest,
			@Nullable final Integer size, @Nullable final Boolean addTieBreaker) {
		super((addTieBreaker == null || addTieBreaker
				? searchRequest.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
				: searchRequest).execute());
		this.searchRequest = searchRequest;
		this.size = getSize(size);
	}

	@Override
	protected ListenableActionFuture<SearchResponse> getNextBatch(@NonNull final SearchResponse searchResponse) {
		return (sortValues == null ? searchRequest : searchRequest.searchAfter(sortValues)).setSize(size).execute();
	}

	@Override
	protected void update(@NonNull final SearchHit searchHit) {
		sortValues = searchHit.getSortValues();
	}

}
