package com.acuumulo.playground.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.acuumulo.playground.graph.GraphConstants.Memento;
import com.acuumulo.playground.graph.GraphConstants.MementoPart;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AccGraphReader {
//	/**
//	 * Find all graph matches.
//	 * @param conn Connector
//	 * @param table String
//	 * @param memento Memento
//	 * @param mementoPart MementoPart
//	 * @param value byte[]
//	 * @param auths List<String> auths
//	 * @return Map<Key,Value>
//	 */
//	public static Map<Key,Value> find(Connector conn, String table, Memento memento, MementoPart mementoPart, byte[] value, List<String> auths){
//
//
//
//	}
//
//	/**
//	 * Find all graph matches for a String value.
//	 * @param conn Connector
//	 * @param table String
//	 * @param memento Memento
//	 * @param mementoPart MementoPart
//	 * @param value String
//	 * @param auths List<String> auths
//	 * @return Map<Key,Value>
//	 */
//	public static Map<Key,Value> find(Connector conn, String table, Memento memento, MementoPart mementoPart, String value, List<String> auths){
//		return find(conn,table,memento,mementoPart,value,auths);
//	}
//
//	/**
//	 * Find all graph matches for a Number value.
//	 * @param conn Connector
//	 * @param table String
//	 * @param memento Memento
//	 * @param mementoPart MementoPart
//	 * @param value Number
//	 * @param auths List<String> auths
//	 * @return Map<Key,Value>
//	 */
//	public static Map<Key,Value> find(Connector conn, String table, Memento memento, MementoPart mementoPart, Number value, List<String> auths){
//		return find(conn,table,memento,mementoPart,value.toString().getBytes(),auths);
//	}
//	
//	/**
//	 * Return the first node starting with the given string.
//	 *
//	 * @param start
//	 * @param scanner
//	 * @return
//	 */
//	public static Optional<String> discoverNode(
//			final String start,
//			final Scanner scanner) {
//
//		scanner.setRange(Range.prefix(start));
//
//		Iterator<Map.Entry<Key, Value>> iter = scanner.iterator();
//		if (!iter.hasNext()) {
//			return Optional.absent();
//		}
//
//		return Optional.of(iter.next().getKey().getColumnQualifier().toString());
//	}
//
//	/**
//	 * Return a list of neighbors for a given node.
//	 *
//	 * @param node
//	 * @param scanner
//	 * @param edgeType
//	 * @return
//	 */
//	public static Iterable<String> getNeighbors(
//			final String node,
//			final Scanner scanner,
//			final String edgeType) {
//
//		scanner.setRange(Range.exact(node));
//
//		if (!edgeType.equals("ALL")) {
//			scanner.fetchColumnFamily(new Text(edgeType));
//		}
//
//		return Iterables.transform(scanner, new Function<Entry<Key, Value>, String>() {
//			@Override
//			public String apply(Entry<Key, Value> f) {
//				return f.getKey().getColumnQualifier().toString();
//			}
//		});
//	}
//
//	/**
//	 * 
//	 * @param neighbors
//	 * @param batchScanner
//	 * @param edgeType
//	 * @return 
//	 */
//	public static Iterable<String> neighborsOfNeighbors(
//			final Iterable<String> neighbors,
//			final BatchScanner batchScanner,
//			final String edgeType) {
//
//		List<Iterable<String>> nextNeighbors = new ArrayList<>();
//
//		// process given neighbors in batches of 100
//		for (List<String> batch : Iterables.partition(neighbors, 100)) {
//			batchScanner.setRanges(Lists.transform(batch, new Function<String, Range>() {
//				@Override
//				public Range apply(String f) {
//					return Range.exact(f);
//				}
//			}));
//
//			if (!edgeType.equals("ALL")) 
//				batchScanner.fetchColumnFamily(new Text(edgeType));
//
//			nextNeighbors.add(Iterables.transform(batchScanner, new Function<Entry<Key, Value>, String>() {
//				@Override
//				public String apply(Entry<Key, Value> f) {
//					return f.getKey().getColumnQualifier().toString();
//				}
//			}));
//		}
//
//		return Sets.newHashSet(Iterables.concat(nextNeighbors));
//	}
}
