package com.thunken.sandow;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.thunken.sandow.spliterator.PaginatingSearchHitSpliterator;
import com.thunken.sandow.spliterator.ScrollingSearchHitSpliterator;
import com.thunken.sandow.spliterator.SearchAfterSearchHitSpliterator;

import lombok.NonNull;

/**
 * A collection backed by an Elasticsearch index.
 *
 * <p>
 * A note on terminology: this interface bridges the Java Collections Framework (where a <i>collection</i> represents a
 * group of objects known as its <i>elements</i>) and Elasticsearch (where an <i>index</i> represents a group of objects
 * known as <i>documents</i>). The code and the documentation of this interface use the terminology of the Java
 * Collections Framework.
 *
 * <p>
 * Design principles:
 * <ul>
 * <li>Interoperability: this interface puts no bounds on the type argument {@code <E>};
 * <li>Minimalism: this interface provides reasonable implementations for most methods using default methods rather than
 * companion abstract classes.
 * </ul>
 *
 * <p>
 * An index contains no duplicate elements. More formally, an index contains no pair of elements <code>e1</code> and
 * <code>e2</code> such that <code>index.getId(e1).equals(index.getId(e2))</code>. Additionally, an index does not allow
 * null elements.
 *
 * <p>
 * Contrary to what is still possible as of Elasticsearch 5.4, this class removes support for querying multiple types in
 * a single index. More precisely, an {@code Index} contains elements of a single type {@code <E>} (see
 * <a href="https://www.elastic.co/blog/index-vs-type">Index vs. Type</a> and
 * <a href="https://github.com/elastic/elasticsearch/issues/15613">Remove support for types?</a> for discussions of the
 * issue). Attempting to query the presence of an ineligible element shall throw an exception, typically
 * {@link NullPointerException} or {@link ClassCastException}. Furthermore, by default, the name of the index in
 * Elasticsearch shall be equal to the elements' type in the index, which is itself derived from the lowercased
 * {@link Class#getSimpleName()} of the elements' class.
 *
 * @implSpec The default method implementations do not apply any synchronization protocol, and all operations performed
 *           with a {@code Client} are asynchronous by nature. If an {@code Index} implementation has a specific
 *           synchronization protocol, then it must override default implementations to apply that protocol.
 *
 * @param <E>
 *            the type of elements in this collection
 * @param <C>
 *            the type of the client used to perform actions against the cluster
 *
 * @see Client
 * @see Collection
 * @see TransportClient
 */
public interface Index<E, C extends Client> extends Collection<E> {

	@Override
	default boolean add(@NonNull final E element) {
		prepareIndex(element).get();
		return true;
	}

	@Override
	default boolean addAll(@NonNull final Collection<? extends E> collection) {
		boolean modified = false;
		for (final E element : collection) {
			if (add(element)) {
				modified = true;
			}
		}
		return modified;
	}

	default List<AnalyzeToken> analyze(@NonNull final String text, @NonNull final String analyzer) {
		return prepareAnalyze(text).setAnalyzer(analyzer).get().getTokens();
	}

	@Override
	default void clear() {
		stream().forEach(this::remove);
	}

	@Override
	default boolean contains(@NonNull final Object object) {
		return contains(getId(getElementClass().cast(object)));
	}

	default boolean contains(@NonNull final String elementId) {
		return prepareGet(elementId).setFetchSource(false).get().isExists();
	}

	@Override
	default boolean containsAll(@NonNull final Collection<?> collection) {
		for (final Object object : collection) {
			if (!contains(object)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Execute a query and get the number of matches for that query.
	 *
	 * @param queryBuilder
	 *            the query to execute
	 * @return the number of matches for that query
	 */
	default long count(final QueryBuilder queryBuilder) {
		return prepareSearch().setQuery(queryBuilder).setSize(0).get().getHits().getTotalHits();
	}

	default DeleteRequest deleteRequest(@NonNull final String elementId) {
		return new DeleteRequest(getName(), getType(), elementId);
	}

	E deserialize(@NonNull final byte[] bytes) throws IOException;

	default E deserialize(@NonNull final SearchHit searchHit) throws IOException {
		return deserialize(BytesReference.toBytes(Objects.requireNonNull(searchHit.getSourceRef())));
	}

	default Optional<E> deserializeOrEmpty(@NonNull final byte[] bytes) {
		try {
			return Optional.of(deserialize(bytes));
		} catch (final IOException e) {
			return Optional.empty();
		}
	}

	default Optional<E> deserializeOrEmpty(@NonNull final SearchHit searchHit) {
		try {
			return Optional.ofNullable(deserialize(searchHit));
		} catch (final IOException e) {
			return Optional.empty();
		}
	}

	default boolean exists() {
		return prepareExists().get().isExists();
	}

	default void flush() {
		prepareFlush().get();
	}

	default Optional<E> get(@NonNull final String elementId) {
		final GetResponse response = prepareGet(elementId).get();
		return response.isExists() ? deserializeOrEmpty(response.getSourceAsBytes()) : Optional.empty();
	}

	/**
	 * Return the client used to perform actions against the cluster.
	 *
	 * @return the client used to perform actions against the cluster
	 */
	C getClient();

	/**
	 * Return the {@code Class} object representing the element type of this collection.
	 *
	 * @return the {@code Class} object representing the element type of this collection
	 */
	Class<E> getElementClass();

	default Optional<GetField> getField(@NonNull final String elementId, @NonNull final String fieldName) {
		final GetResponse response = prepareGet(elementId).setFetchSource(fieldName, null).get();
		return response.isExists() ? Optional.ofNullable(response.getField(fieldName)) : Optional.empty();
	}

	/**
	 * Return the unique {@code String} ID of the given element.
	 *
	 * @param element
	 *            an object of type {@code <E>}, that may or may not be part of this collection
	 * @return the unique id of the given element
	 */
	String getId(@NonNull E element);

	/**
	 * Return the name of this collection in Elasticsearch.
	 *
	 * @return the name of this collection in Elasticsearch
	 */
	default String getName() {
		return getType();
	}

	default int getPageSize() {
		return 10;
	}

	default Scroll getScroll() {
		return new Scroll(new TimeValue(1L, TimeUnit.MINUTES));
	}

	default Optional<ByteSizeValue> getSize(@NonNull final String elementId) {
		return getField(elementId, "_size").map(GetField::getValue).map(Object::toString).map(Long::parseLong)
				.map(ByteSizeValue::new);
	}

	/**
	 * Return the type of this collection's elements in Elasticsearch.
	 *
	 * @implSpec The default implementation gets the return value from the lowercased {@link Class#getSimpleName()} of
	 *           {@link Index#getElementClass()}.
	 *
	 * @return the name of this collection's elements in Elasticsearch
	 */
	default String getType() {
		return getElementClass().getSimpleName().toLowerCase(Locale.ROOT);
	}

	default OptionalLong getVersion(@NonNull final String elementId) {
		return getField(elementId, "_version").map(GetField::getValue).map(Object::toString).map(Long::parseLong)
				.map(OptionalLong::of).orElse(OptionalLong.empty());
	}

	/**
	 * Return the content type of data returned by {@link Index#serialize} and consumed by {@link Index#deserialize}.
	 *
	 * @return the content type of data returned by {@link Index#serialize} and consumed by {@link Index#deserialize}
	 */
	XContentType getXContentType();

	default IndexRequest indexRequest(@NonNull final E element) {
		return indexRequest(element, getId(element));
	}

	default IndexRequest indexRequest(@NonNull final E element, final String elementId) {
		final byte[] bytes;
		try {
			bytes = serialize(element);
		} catch (final IOException e) {
			throw new IllegalArgumentException(e);
		}
		return new IndexRequest(getName(), getType(), elementId).source(bytes, getXContentType());
	}

	@Override
	default boolean isEmpty() {
		return sizeAsLong() == 0L;
	}

	@Override
	default Iterator<E> iterator() {
		return stream().iterator();
	}

	@Override
	default Stream<E> parallelStream() {
		return stream();
	}

	default AnalyzeRequestBuilder prepareAnalyze(@NonNull final String text) {
		return getClient().admin().indices().prepareAnalyze(getName(), text);
	}

	default DeleteRequestBuilder prepareDelete(@NonNull final String elementId) {
		return getClient().prepareDelete(getName(), getType(), elementId);
	}

	default IndicesExistsRequestBuilder prepareExists() {
		return getClient().admin().indices().prepareExists(getName());
	}

	default FlushRequestBuilder prepareFlush() {
		return getClient().admin().indices().prepareFlush(getName()).setWaitIfOngoing(true);
	}

	default GetRequestBuilder prepareGet(@NonNull final String elementId) {
		return getClient().prepareGet(getName(), getType(), elementId).setFetchSource(true);
	}

	default IndexRequestBuilder prepareIndex() {
		return getClient().prepareIndex(getName(), getType());
	}

	default IndexRequestBuilder prepareIndex(@NonNull final String elementId) {
		return getClient().prepareIndex(getName(), getType(), elementId);
	}

	default IndexRequestBuilder prepareIndex(@NonNull final E element) {
		final byte[] bytes;
		try {
			bytes = serialize(element);
		} catch (final IOException e) {
			throw new IllegalArgumentException(e);
		}
		return prepareIndex(getId(element)).setSource(bytes, getXContentType());
	}

	default PaginatingSearchHitSpliterator.PaginatingSearchHitSpliteratorBuilder preparePaginatingSpliterator() {
		return PaginatingSearchHitSpliterator.builder().size(getPageSize());
	}

	default RefreshRequestBuilder prepareRefresh() {
		return getClient().admin().indices().prepareRefresh(getName());
	}

	default ScrollingSearchHitSpliterator.ScrollingSearchHitSpliteratorBuilder prepareScrollingSpliterator() {
		return ScrollingSearchHitSpliterator.builder().client(getClient()).scroll(getScroll()).size(getPageSize());
	}

	default SearchRequestBuilder prepareSearch() {
		return getClient().prepareSearch(getName()).setFetchSource(true).setTypes(getType());
	}

	default SearchAfterSearchHitSpliterator.SearchAfterSearchHitSpliteratorBuilder prepareSearchAfterSpliterator() {
		return SearchAfterSearchHitSpliterator.builder().addTieBreaker(true).size(getPageSize());
	}

	default SearchRequestBuilder prepareSearchWithScroll() {
		return prepareSearch().addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC).setScroll(getScroll())
				.setSize(getPageSize());
	}

	default void refresh() {
		prepareRefresh().get();
	}

	@Override
	default boolean remove(@NonNull final Object object) {
		return remove(getId(getElementClass().cast(object)));
	}

	default boolean remove(@NonNull final String elementId) {
		if (contains(elementId)) {
			prepareDelete(elementId).get();
			return true;
		}
		return false;
	}

	@Override
	default boolean removeAll(@NonNull final Collection<?> collection) {
		boolean modified = false;
		for (final Object object : collection) {
			if (remove(object)) {
				modified = true;
			}
		}
		return modified;
	}

	@Override
	default boolean removeIf(@NonNull final Predicate<? super E> filter) {
		return stream().filter(filter).map(this::remove).reduce(Boolean::logicalOr).orElse(false);
	}

	@Override
	default boolean retainAll(@NonNull final Collection<?> collection) {
		return removeIf(element -> !collection.contains(element));
	}

	byte[] serialize(@NonNull final E element) throws IOException;

	/**
	 * {@inheritDoc}
	 */
	@Override
	default int size() {
		final long size = sizeAsLong();
		return size < Integer.MAX_VALUE ? (int) size : Integer.MAX_VALUE;
	}

	/**
	 * Returns the number of elements in this collection as a {@code long}.
	 *
	 * @return the number of elements in this collection
	 */
	default long sizeAsLong() {
		return exists() ? count(null) : 0L;
	}

	/**
	 * Returns a sequential {@code Stream} with this collection as its source.
	 *
	 * @return a sequential {@code Stream} over the elements in this collection
	 */
	@Override
	default Stream<E> stream() {
		return stream(null);
	}

	/**
	 * Returns a sequential {@code Stream} with a subset of this collection as its source, namely the elements that
	 * match the given search query.
	 *
	 * @param queryBuilder
	 *            the search query to execute
	 * @return a sequential {@code Stream} over the elements in this collection that
	 */
	default Stream<E> stream(final QueryBuilder queryBuilder) {
		return streamSearchHits(queryBuilder).map(this::deserializeOrEmpty).filter(Optional::isPresent)
				.map(Optional::get);
	}

	default Stream<String> streamIds() {
		return streamIds(null);
	}

	default Stream<String> streamIds(final QueryBuilder queryBuilder) {
		return streamSearchHits(queryBuilder).map(SearchHit::getId).filter(Objects::nonNull);
	}

	default Stream<SearchHit> streamSearchHits() {
		return streamSearchHits(null);
	}

	default Stream<SearchHit> streamSearchHits(final QueryBuilder queryBuilder) {
		return StreamSupport.stream(
				prepareScrollingSpliterator().searchRequest(prepareSearch().setQuery(queryBuilder)).build(), false);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	default Object[] toArray() {
		return stream().toArray();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	@SuppressWarnings("unchecked")
	default <T> T[] toArray(@NonNull final T[] array) {
		return stream().toArray(size -> (T[]) Array.newInstance(array.getClass().getComponentType(), size));
	}

}
