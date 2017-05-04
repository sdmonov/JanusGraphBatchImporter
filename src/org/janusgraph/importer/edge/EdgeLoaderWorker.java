package org.janusgraph.importer.edge;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.importer.util.Constants;
import org.janusgraph.importer.util.Worker;

public class EdgeLoaderWorker extends Worker {
	private final UUID myID = UUID.randomUUID();

	private final Spliterator<CSVRecord> records;
	private final JanusGraph graph;
	private JanusGraphTransaction graphTransaction;

	private final Map<String, Object> propertiesMap;
	private final String defaultEdgeLabel;
	private String edgeLabelFieldName;

	private Logger log = Logger.getLogger(EdgeLoaderWorker.class);

	public EdgeLoaderWorker(final Spliterator<CSVRecord> records, final Map<String, Object> propertiesMap,
			final JanusGraph graph) {
		this.records = records;
		this.graph = graph;
		this.propertiesMap = propertiesMap;
		this.defaultEdgeLabel = (String) propertiesMap.get(Constants.EDGE_LABEL_MAPPING);
		this.edgeLabelFieldName = null;

		if (propertiesMap.values().contains(Constants.EDGE_LABEL_MAPPING)) {
			for (String propName : propertiesMap.keySet()) {
				if (Constants.EDGE_LABEL_MAPPING.equals(propertiesMap.get(propName))) {
					this.edgeLabelFieldName = propName;
					break;
				}
			}
		}
	}
	
	private void acceptRecord(CSVRecord record) throws Exception {
		String edgeLabel = defaultEdgeLabel;
		if (edgeLabelFieldName != null) {
			edgeLabel = record.get(edgeLabelFieldName);
		}

		// Get the left and right edge labels

		Map<String, String> leftEdge = (Map<String, String>) propertiesMap.get(Constants.EDGE_LEFT_MAPPING);
		String leftEdgeFieldName = leftEdge.keySet().iterator().next();
		String leftVertex = leftEdge.get(leftEdgeFieldName);
		String leftVertexLabel = leftVertex.substring(0, leftVertex.indexOf('.'));
		String leftVertexFieldName = leftVertex.substring(leftVertex.indexOf('.') + 1);

		Map<String, String> rightEdge = (Map<String, String>) propertiesMap.get(Constants.EDGE_RIGHT_MAPPING);
		String rightEdgeFieldName = rightEdge.keySet().iterator().next();
		String rightVertex = rightEdge.get(rightEdgeFieldName);
		String rightVertexLabel = rightVertex.substring(0, rightVertex.indexOf('.'));
		String rightVertexFieldName = rightVertex.substring(rightVertex.indexOf('.') + 1);

		Iterator<Vertex> node_1 = graphTransaction.traversal().V().has(leftVertexLabel, leftVertexFieldName,
				record.get(leftEdgeFieldName));
		Iterator<Vertex> node_2 = graphTransaction.traversal().V().has(rightVertexLabel, rightVertexFieldName,
				record.get(rightEdgeFieldName));

		if (node_1.hasNext() && node_2.hasNext()) {
			Vertex v1 = node_1.next();
			Vertex v2 = node_2.next();
			Edge edge = v1.addEdge(edgeLabel, v2);

			// set the properties of the edge
			for (String column : record.toMap().keySet()) {
				String value = record.get(column);
				// If value="" or it is edge label then skip it
				if (value == null || value.length() == 0 || column.equals(edgeLabelFieldName)
						|| column.equals(leftEdgeFieldName) || column.equals(rightEdgeFieldName))
					continue;

				String propName = (String) propertiesMap.get(column);
				if (propName == null) {
					// log.info("Thread " + myID + ".Cannot find property name
					// for column " + column
					// + " in the properties map. Using the column name as
					// default.");
//					propName = column;
					continue;
				}

				// Update property only if it does not exist already
				if (!edge.properties(propName).hasNext()) {
					// TODO Convert properties between data types. e.g. Date
					edge.property(propName, value);
				}
			}
		}
	}

	public UUID getMyID() {
		return myID;
	}

	@Override
	public void run() {
		log.info("Starting new thread " + myID + " to import " + records.estimateSize() + " edge records.");

		// Start new graph transaction
		graphTransaction = graph.newTransaction();
		records.forEachRemaining(new Consumer<CSVRecord>() {
			@Override
			public void accept(CSVRecord record) {
				try {
					acceptRecord(record);
				} catch (Exception e) {
					log.error("Thread " + myID + ". Exception during record import.", e);
				}
			}

		});
		graphTransaction.commit();
		graphTransaction.close();

		log.info("Finished thread " + myID);
	}

}
