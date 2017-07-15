package com.thunken.sandow.spliterator;

import java.util.Spliterator;
import java.util.function.Consumer;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Synchronized;

/**
 * Base class for implementations of {@code Spliterator} that retrieve
 * potentially large numbers of results from an Elasticsearch index.
 *
 * @see SearchHit
 * @see Spliterator
 */
public abstract class SearchHitSpliterator implements Spliterator<SearchHit> {

	@Getter(AccessLevel.PROTECTED)
	private int cursor;

	private ListenableActionFuture<? extends SearchResponse> nextBatch;

	private SearchHit[] searchHits = new SearchHit[0];

	protected SearchHitSpliterator(final ListenableActionFuture<? extends SearchResponse> firstBatch) {
		nextBatch = firstBatch;
	}

	@Override
	public int characteristics() {
		return Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.ORDERED;
	}

	protected void close() {
		/* NO OP */
	}

	@Override
	public long estimateSize() {
		return Long.MAX_VALUE;
	}

	protected abstract ListenableActionFuture<? extends SearchResponse> getNextBatch(SearchResponse searchResponse);

	@Override
	@Synchronized
	public boolean tryAdvance(@NonNull final Consumer<? super SearchHit> action) {
		if (cursor < searchHits.length) {
			final SearchHit searchHit = searchHits[cursor++];
			update(searchHit);
			action.accept(searchHit);
			return true;
		}
		if (nextBatch == null) {
			close();
			return false;
		}
		final SearchResponse searchResponse = nextBatch.actionGet();
		cursor = 0;
		searchHits = searchResponse.getHits().getHits();
		nextBatch = getNextBatch(searchResponse);
		if (searchHits.length == 0) {
			close();
			return false;
		}
		return tryAdvance(action);
	}

	@Override
	public Spliterator<SearchHit> trySplit() {
		return null;
	}

	protected void update(@NonNull final SearchHit searchHit) {
		/* NO OP */
	}

}
