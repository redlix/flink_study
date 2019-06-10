package com.red.flink.java.course05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * <pre>
 *  Version         Date            Author          Description
 * ------------------------------------------------------------
 *  1.0.0           2019/06/10     red        -
 * </pre>
 *
 * @author red
 * @version 1.0.0 2019-06-10 08:58
 * @since 1.0.0
 */
public class JavaCustomParallelSourceFunction implements ParallelSourceFunction<Long> {
	private boolean isRunning = true;
	private long count = 1;

	@Override
	public void run(SourceContext<Long> sourceContext) throws Exception {
		while (isRunning) {
			sourceContext.collect(count);
			count += 1;
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
