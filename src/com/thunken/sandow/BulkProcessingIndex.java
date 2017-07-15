package com.thunken.sandow;

import java.util.Collection;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;

import lombok.NonNull;

/**
 * A collection backed by an Elasticsearch index and a {@code BulkProcessor}.
 *
 * @implSpec The default method implementations do not apply any synchronization
 *           protocol, and all operations performed with a {@code Client} are
 *           asynchronous by nature. If an {@code Index} implementation has a
 *           specific synchronization protocol, then it must override default
 *           implementations to apply that protocol.
 *
 * @param <E>
 *            the type of elements in this collection
 * @param <C>
 *            the type of the client used to perform actions against the cluster
 *
 * @see Client
 * @see Collection
 * @see Index
 * @see TransportClient
 */
public interface BulkProcessingIndex<E, C extends Client> extends Index<E, C> {

	@Override
	default boolean add(@NonNull final E indexable) {
		getBulkProcessor().add(indexRequest(indexable));
		return true;
	}

	@Override
	default void flush() {
		getBulkProcessor().flush();
		Index.super.flush();
	}

	BulkProcessor getBulkProcessor();

	@Override
	default void refresh() {
		getBulkProcessor().flush();
		Index.super.refresh();
	}

	@Override
	default boolean remove(@NonNull final String elementId) {
		if (contains(elementId)) {
			getBulkProcessor().add(deleteRequest(elementId));
			return true;
		}
		return false;
	}

}
