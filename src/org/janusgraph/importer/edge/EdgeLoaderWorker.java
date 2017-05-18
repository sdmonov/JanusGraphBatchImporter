package org.janusgraph.importer.edge;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.importer.util.BatchHelper;
import org.janusgraph.importer.util.Constants;
import org.janusgraph.importer.util.Worker;

public class EdgeLoaderWorker extends Worker {
	private final UUID myID = UUID.randomUUID();

	private JanusGraphTransaction graphTransaction;
	private long currentRecord;
	private final String defaultEdgeLabel;
	private String edgeLabelFieldName;

	private Logger log = Logger.getLogger(EdgeLoaderWorker.class);

	public EdgeLoaderWorker(final Iterator<Map<String, String>> records, final Map<String, Object> propertiesMap,
			final JanusGraph graph) {
		super(records, propertiesMap, graph);

		this.currentRecord = 0;
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

	private void acceptRecord(Map<String, String> record) throws Exception {
		String edgeLabel = defaultEdgeLabel;
		if (edgeLabelFieldName != null) {
			edgeLabel = record.get(edgeLabelFieldName);
		}

		// Get the left and right edge labels

		Map<String, String> leftEdge = (Map<String, String>) getPropertiesMap().get(Constants.EDGE_LEFT_MAPPING);
		String leftEdgeFieldName = leftEdge.keySet().iterator().next();
		String leftVertex = leftEdge.get(leftEdgeFieldName);
		String leftVertexLabel = leftVertex.substring(0, leftVertex.indexOf('.'));
		String leftVertexFieldName = leftVertex.substring(leftVertex.indexOf('.') + 1);

		Map<String, String> rightEdge = (Map<String, String>) getPropertiesMap().get(Constants.EDGE_RIGHT_MAPPING);
		String rightEdgeFieldName = rightEdge.keySet().iterator().next();
		String rightVertex = rightEdge.get(rightEdgeFieldName);
		String rightVertexLabel = rightVertex.substring(0, rightVertex.indexOf('.'));
		String rightVertexFieldName = rightVertex.substring(rightVertex.indexOf('.') + 1);

		GraphTraversalSource traversal = getGraph().traversal();

		Iterator<Vertex> node_1 = traversal.V().has(leftVertexLabel, leftVertexFieldName,
				record.get(leftEdgeFieldName));
		Iterator<Vertex> node_2 = traversal.V().has(rightVertexLabel, rightVertexFieldName,
				record.get(rightEdgeFieldName));

		try {
			node_1.hasNext();
		} catch (Exception e) {
			Thread.currentThread().sleep(500);
			node_1 = traversal.V().has(leftVertexLabel, leftVertexFieldName, record.get(leftEdgeFieldName));
			try {
				node_1.hasNext();
			} catch (Exception e1) {
				Thread.currentThread().sleep(500);
				node_1 = traversal.V().has(leftVertexLabel, leftVertexFieldName, record.get(leftEdgeFieldName));
			}
		}

		try {
			node_2.hasNext();
		} catch (Exception e) {
			Thread.currentThread().sleep(500);
			node_2 = traversal.V().has(rightVertexLabel, rightVertexFieldName, record.get(rightEdgeFieldName));
			try {
				node_2.hasNext();
			} catch (Exception e1) {
				Thread.currentThread().sleep(500);
				node_2 = traversal.V().has(rightVertexLabel, rightVertexFieldName, record.get(rightEdgeFieldName));
			}
		}

		try {
			if (node_1.hasNext() && node_2.hasNext()) {
				Vertex v1 = node_1.next();
				Vertex v2 = node_2.next();
				Edge edge = v1.addEdge(edgeLabel, v2);

				// set the properties of the edge
				for (String column : record.keySet()) {
					String value = record.get(column);
					// If value="" or it is edge label then skip it
					if (value == null || value.length() == 0 || column.equals(edgeLabelFieldName)
							|| column.equals(leftEdgeFieldName) || column.equals(rightEdgeFieldName))
						continue;

					String propName = (String) getPropertiesMap().get(column);
					if (propName == null) {
						// log.info("Thread " + myID + ".Cannot find
						// property name
						// for column " + column
						// + " in the properties map. Using the column name
						// as
						// default.");
						// propName = column;
						continue;
					}

					// Update property only if it does not exist already
					if (!edge.properties(propName).hasNext()) {
						// TODO Convert properties between data types. e.g.
						// Date
						Object convertedValue = BatchHelper.convertPropertyValue(value,
								graphTransaction.getPropertyKey(propName).dataType());
						edge.property(propName, convertedValue);
					}
				}
			}
		} catch (Exception e) {
			log.error(e);
			// System.exit(0);
		}
		if (currentRecord % Constants.DEFAULT_EDGE_COMMIT_COUNT == 0) {
			graphTransaction.commit();
			graphTransaction.close();
			graphTransaction = getGraph().newTransaction();
		}
		currentRecord++;
	}

	public UUID getMyID() {
		return myID;
	}

	@Override
	public void run() {
		log.info("Starting new thread " + myID);

		// Start new graph transaction
		graphTransaction = getGraph().newTransaction();
		getRecords().forEachRemaining(new Consumer<Map<String, String>>() {
			@Override
			public void accept(Map<String, String> record) {
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
