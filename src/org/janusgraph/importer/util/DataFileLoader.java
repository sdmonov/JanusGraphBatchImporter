package org.janusgraph.importer.util;

import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Spliterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.janusgraph.core.JanusGraph;

public class DataFileLoader {
	private JanusGraph graph;
	private Map<String, Object> propertiesMap;
	private Class<Worker> workerClass;

	private Logger log = Logger.getLogger(DataFileLoader.class);

	public DataFileLoader(JanusGraph graph, Class<Worker> workerClass) {
		this.graph = graph;
		this.workerClass = workerClass;
	}

	private void startWorkers(Spliterator<CSVRecord> spliterator, long targetRecordCount, WorkerPool workers) throws Exception {
		Spliterator<CSVRecord> newSpliterator;
		while ((spliterator.estimateSize() > targetRecordCount)
				&& ((newSpliterator = spliterator.trySplit()) != null)) {
			startWorkers(newSpliterator, targetRecordCount, workers);
		}
		Constructor<Worker> constructor = workerClass.getConstructor(Spliterator.class, Map.class,JanusGraph.class);
		Worker worker = constructor.newInstance(spliterator, propertiesMap, graph);
		workers.submit(worker);
	}

	public void loadFile(String fileName, Map<String, Object> propertiesMap, WorkerPool workers)
			throws Exception {
		log.info("Loading Vertexes from " + fileName);
		long linesCount = BatchHelper.countLines(fileName);

		this.propertiesMap = propertiesMap;

		Reader in = new FileReader(fileName);
		Spliterator<CSVRecord> spliter = CSVFormat.EXCEL.withHeader().parse(in).spliterator();

		startWorkers(spliter, (linesCount / 8), workers);
	}
}
