package zx.soft.opentsdb.reporter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import zx.soft.opentsdb.client.OpenTsdbClient;
import zx.soft.opentsdb.metric.OpenTsdbMetric;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * Reporter实现类，将Metric数据发送到OpenTSDB服务器上
 *
 * @author wanggang
 *
 */
public class OpenTsdbReporter extends ScheduledReporter {

	// OpenTSDB客户端
	private final OpenTsdbClient opentsdb;
	// 时钟对象
	private final Clock clock;
	// 所有的Metrix前缀名
	private final String prefix;
	// Tags列表
	private final Map<String, String> tags;

	/**
	 * 返回{@link OpenTsdbReporter}的{@link Builder}实例
	 *
	 * @param registry report注册类
	 * @return {@link OpenTsdbReporter}的{@link Builder}实例
	 */
	public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry);
	}

	private OpenTsdbReporter(MetricRegistry registry, OpenTsdbClient opentsdb, Clock clock, String prefix,
			TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter, Map<String, String> tags) {
		super(registry, "opentsdb-reporter", filter, rateUnit, durationUnit);
		this.opentsdb = opentsdb;
		this.clock = clock;
		this.prefix = prefix;
		this.tags = tags;
	}

	/**
	 * {@link OpenTsdbReporter}实例化的Builder类
	 *
	 * 默认不使用前缀名，而是使用clock时钟将比率转换成events/second，
	 * 将时间长度转换成毫秒，并且不过率metric。
	 */
	public static class Builder {

		// Metric注册类
		private final MetricRegistry registry;
		// 时钟
		private Clock clock;
		// 前缀名
		private String prefix;
		// 比率单位
		private TimeUnit rateUnit;
		// 时长单位
		private TimeUnit durationUnit;
		// Metric过滤器
		private MetricFilter filter;
		// Tags列表
		private Map<String, String> tags;
		// 批量大小
		private int batchSize;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
			this.batchSize = OpenTsdbClient.DEFAULT_BATCH_SIZE_LIMIT;
		}

		public Builder withClock(Clock clock) {
			this.clock = clock;
			return this;
		}

		/**
		 * 根据给定的前缀名给所有的Metric名字加上前缀
		 */
		public Builder prefixedWith(String prefix) {
			this.prefix = prefix;
			return this;
		}

		/**
		 * 将比例转换成给定的时间单位
		 */
		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		/**
		 * 将时间长度转换成给定的时间单位
		 */
		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		/**
		 * 对符合过滤调剂的Metric进行Report
		 */
		public Builder filter(MetricFilter filter) {
			this.filter = filter;
			return this;
		}

		/**
		 * 将Tags加到所有需要Report的Metric中
		 */
		public Builder withTags(Map<String, String> tags) {
			this.tags = tags;
			return this;
		}

		/**
		 * 制定批量发送的Metric数量
		 */
		public Builder withBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public OpenTsdbReporter build(OpenTsdbClient opentsdb) {
			opentsdb.setBatchSizeLimit(batchSize);
			return new OpenTsdbReporter(registry, opentsdb, clock, prefix, rateUnit, durationUnit, filter, tags);
		}

	}

	/**
	 * Metric集合
	 */
	private static class MetricsCollector {

		// 前缀名
		private final String prefix;
		// Tags列表
		private final Map<String, String> tags;
		// 时间戳
		private final long timestamp;
		// Metric集合
		private final Set<OpenTsdbMetric> metrics = new HashSet<>();

		private MetricsCollector(String prefix, Map<String, String> tags, long timestamp) {
			this.prefix = prefix;
			this.tags = tags;
			this.timestamp = timestamp;
		}

		public static MetricsCollector createNew(String prefix, Map<String, String> tags, long timestamp) {
			return new MetricsCollector(prefix, tags, timestamp);
		}

		public MetricsCollector addMetric(String metricName, Object value) {
			this.metrics.add(OpenTsdbMetric.named(MetricRegistry.name(prefix, metricName)).withTimestamp(timestamp)
					.withValue(value).withTags(tags).build());
			return this;
		}

		public Set<OpenTsdbMetric> build() {
			return metrics;
		}

	}

	/**
	 * Report操作
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
			SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

		final long timestamp = clock.getTime() / 1000;

		final Set<OpenTsdbMetric> metrics = new HashSet<>();

		for (Map.Entry<String, Gauge> g : gauges.entrySet()) {
			if (g.getValue().getValue() instanceof Collection && ((Collection) g.getValue().getValue()).isEmpty()) {
				continue;
			}
			metrics.add(buildGauge(g.getKey(), g.getValue(), timestamp));
		}

		for (Map.Entry<String, Counter> entry : counters.entrySet()) {
			metrics.add(buildCounter(entry.getKey(), entry.getValue(), timestamp));
		}

		for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
			metrics.addAll(buildHistograms(entry.getKey(), entry.getValue(), timestamp));
		}

		for (Map.Entry<String, Meter> entry : meters.entrySet()) {
			metrics.addAll(buildMeters(entry.getKey(), entry.getValue(), timestamp));
		}

		for (Map.Entry<String, Timer> entry : timers.entrySet()) {
			metrics.addAll(buildTimers(entry.getKey(), entry.getValue(), timestamp));
		}

		opentsdb.send(metrics);
	}

	private Set<OpenTsdbMetric> buildTimers(String name, Timer timer, long timestamp) {

		final MetricsCollector collector = MetricsCollector.createNew(prefix(name), tags, timestamp);
		final Snapshot snapshot = timer.getSnapshot();

		return collector
				.addMetric("count", timer.getCount())
				// 转换比率
				.addMetric("m15", convertRate(timer.getFifteenMinuteRate()))
				.addMetric("m5", convertRate(timer.getFiveMinuteRate()))
				.addMetric("m1", convertRate(timer.getOneMinuteRate()))
				.addMetric("mean_rate", convertRate(timer.getMeanRate()))
				// 转换时间长度
				.addMetric("max", convertDuration(snapshot.getMax()))
				.addMetric("min", convertDuration(snapshot.getMin()))
				.addMetric("mean", convertDuration(snapshot.getMean()))
				.addMetric("stddev", convertDuration(snapshot.getStdDev()))
				.addMetric("median", convertDuration(snapshot.getMedian()))
				.addMetric("p75", convertDuration(snapshot.get75thPercentile()))
				.addMetric("p95", convertDuration(snapshot.get95thPercentile()))
				.addMetric("p98", convertDuration(snapshot.get98thPercentile()))
				.addMetric("p99", convertDuration(snapshot.get99thPercentile()))
				.addMetric("p999", convertDuration(snapshot.get999thPercentile())).build();
	}

	private Set<OpenTsdbMetric> buildHistograms(String name, Histogram histogram, long timestamp) {

		final MetricsCollector collector = MetricsCollector.createNew(prefix(name), tags, timestamp);
		final Snapshot snapshot = histogram.getSnapshot();

		return collector.addMetric("count", histogram.getCount()).addMetric("max", snapshot.getMax())
				.addMetric("min", snapshot.getMin()).addMetric("mean", snapshot.getMean())
				.addMetric("stddev", snapshot.getStdDev()).addMetric("median", snapshot.getMedian())
				.addMetric("p75", snapshot.get75thPercentile()).addMetric("p95", snapshot.get95thPercentile())
				.addMetric("p98", snapshot.get98thPercentile()).addMetric("p99", snapshot.get99thPercentile())
				.addMetric("p999", snapshot.get999thPercentile()).build();
	}

	private Set<OpenTsdbMetric> buildMeters(String name, Meter meter, long timestamp) {

		final MetricsCollector collector = MetricsCollector.createNew(prefix(name), tags, timestamp);

		return collector
				.addMetric("count", meter.getCount())
				// convert rate
				.addMetric("mean_rate", convertRate(meter.getMeanRate()))
				.addMetric("m1", convertRate(meter.getOneMinuteRate()))
				.addMetric("m5", convertRate(meter.getFiveMinuteRate()))
				.addMetric("m15", convertRate(meter.getFifteenMinuteRate())).build();
	}

	private OpenTsdbMetric buildCounter(String name, Counter counter, long timestamp) {
		return OpenTsdbMetric.named(prefix(name, "count")).withTimestamp(timestamp).withValue(counter.getCount())
				.withTags(tags).build();
	}

	@SuppressWarnings("rawtypes")
	private OpenTsdbMetric buildGauge(String name, Gauge gauge, long timestamp) {
		return OpenTsdbMetric.named(prefix(name, "value")).withValue(gauge.getValue()).withTimestamp(timestamp)
				.withTags(tags).build();
	}

	private String prefix(String... components) {
		return MetricRegistry.name(prefix, components);
	}

}
