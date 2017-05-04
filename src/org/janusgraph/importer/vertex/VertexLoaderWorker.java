package org.janusgraph.importer.vertex;

import java.util.Map;
import java.util.Spliterator;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.importer.util.Constants;
import org.janusgraph.importer.util.Worker;

public class VertexLoaderWorker extends Worker {
	private final UUID myID = UUID.randomUUID();

	private final Spliterator<CSVRecord> records;
	private final JanusGraph graph;
	private JanusGraphTransaction graphTransaction;

	private final Map<String, Object> propertiesMap;
	private final String defaultVertexLabel;
	private String vertexLabelFieldName;

	private Logger log = Logger.getLogger(VertexLoaderWorker.class);

	public VertexLoaderWorker(final Spliterator<CSVRecord> records, final Map<String, Object> propertiesMap,
			final JanusGraph graph) {
		this.records = records;
		this.graph = graph;
		this.propertiesMap = propertiesMap;
		this.defaultVertexLabel = (String) propertiesMap.get(Constants.VERTEX_LABEL_MAPPING);
		this.vertexLabelFieldName = null;

		if (propertiesMap.values().contains(Constants.VERTEX_LABEL_MAPPING)) {
			// find the vertex
			for (String propName : propertiesMap.keySet()) {
				if (Constants.VERTEX_LABEL_MAPPING.equals(propertiesMap.get(propName))) {
					this.vertexLabelFieldName = propName;
					break;
				}
			}
		}
	}

	private void acceptRecord(CSVRecord record) throws Exception {
		String vertexLabel = defaultVertexLabel;
		if (vertexLabelFieldName != null) {
			vertexLabel = record.get(vertexLabelFieldName);
		}
		JanusGraphVertex v = graphTransaction.addVertex(vertexLabel);

		// set the properties of the vertex
		for (String column : record.toMap().keySet()) {
			String value = record.get(column);
			// If value="" or it is a vertex label then skip it
			if (value == null || value.length() == 0 || column.equals(vertexLabelFieldName))
				continue;

			String propName = (String) propertiesMap.get(column);
			if (propName == null) {
//				log.info("Thread " + myID + ".Cannot find property name for column " + column
//						+ " in the properties map. Using the column name as default.");
				continue;
//				propName = column;
			}

			// Update property only if it does not exist already
			if (!v.properties(propName).hasNext()) {
				// TODO Convert properties between data types. e.g. Date
				v.property(propName, value);
			}
		}
	}
	
	public UUID getMyID() {
		return myID;
	}
	
	@Override
	public void run() {
		log.info("Starting new thread " + myID + " to import " + records.estimateSize() + " records.");

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
