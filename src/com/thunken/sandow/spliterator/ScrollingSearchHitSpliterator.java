package com.thunken.sandow.spliterator;

import java.util.Spliterator;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Specialized implementation of {@code Spliterator} that uses scrolling to
 * retrieve potentially large numbers of results from a single search request in
 * an Elasticsearch index.
 *
 * <p>
 * See <a href=
 * "https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Elasticsearch
 * Reference: Scroll</a> for more information.
 *
 * @see SearchHit
 * @see Spliterator
 */
@Slf4j
public class ScrollingSearchHitSpliterator extends SearchHitSpliterator {

	@NonNull
	private final Client client;

	@NonNull
	private final Scroll scroll;

	private String scrollId;

	@Builder.Default
	private int size = 10;

	@Builder
	private ScrollingSearchHitSpliterator(@NonNull final SearchRequestBuilder searchRequest,
			@NonNull final Client client, @NonNull final Scroll scroll, final int size) {
		super(searchRequest.setScroll(scroll).setSize(size).execute());
		this.client = client;
		this.scroll = scroll;
	}

	@Override
	protected void close() {
		if (scrollId != null && !client.prepareClearScroll().addScrollId(scrollId).get().isSucceeded()) {
			log.warn("{}: clear scroll request for scroll id [{}] did not succeed", this, scrollId);
		}
	}

	@Override
	protected ListenableActionFuture<SearchResponse> getNextBatch(@NonNull final SearchResponse searchResponse) {
		return client.prepareSearchScroll(scrollId = searchResponse.getScrollId()).setScroll(scroll).execute();
	}

}
