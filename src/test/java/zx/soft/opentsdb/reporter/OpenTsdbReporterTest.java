package zx.soft.opentsdb.reporter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import zx.soft.opentsdb.client.OpenTsdbClient;
import zx.soft.opentsdb.metric.OpenTsdbMetric;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

@SuppressWarnings({ "rawtypes", "unchecked" })
@RunWith(MockitoJUnitRunner.class)
public class OpenTsdbReporterTest {

	private OpenTsdbReporter reporter;

	@Mock
	private OpenTsdbClient opentsdb;

	@Mock
	private MetricRegistry registry;

	@Mock
	private Gauge gauge;

	@Mock
	private Counter counter;

	@Mock
	private Clock clock;

	private final long timestamp = 1000198;

	private ArgumentCaptor<Set> captor;

	@Before
	public void setUp() throws Exception {
		captor = ArgumentCaptor.forClass(Set.class);
		reporter = OpenTsdbReporter.forRegistry(registry).withClock(clock).prefixedWith("prefix")
				.convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL)
				.withTags(Collections.singletonMap("foo", "bar")).withBatchSize(100).build(opentsdb);

		when(clock.getTime()).thenReturn(timestamp * 1000);
	}

	@Test
	public void testReportGauges() {
		when(gauge.getValue()).thenReturn(1L);
		reporter.report(this.map("gauge", gauge), this.<Counter> map(), this.<Histogram> map(), this.<Meter> map(),
				this.<Timer> map());
		verify(opentsdb).send(captor.capture());

		final Set<OpenTsdbMetric> metrics = captor.getValue();
		assertEquals(1, metrics.size());
		OpenTsdbMetric metric = metrics.iterator().next();
		assertEquals("prefix.gauge.value", metric.getMetric());
		assertEquals(1L, metric.getValue());
		assertEquals((Long) timestamp, metric.getTimestamp());
	}

	@Test
	public void testReportCounters() {

		when(counter.getCount()).thenReturn(2L);
		reporter.report(this.<Gauge> map(), this.map("counter", counter), this.<Histogram> map(), this.<Meter> map(),
				this.<Timer> map());
		verify(opentsdb).send(captor.capture());

		final Set<OpenTsdbMetric> metrics = captor.getValue();
		assertEquals(1, metrics.size());
		OpenTsdbMetric metric = metrics.iterator().next();
		assertEquals("prefix.counter.count", metric.getMetric());
		assertEquals((Long) timestamp, metric.getTimestamp());
		assertEquals(2L, metric.getValue());
	}

	@Test
	public void testReportHistogram() {

		final Histogram histogram = mock(Histogram.class);
		when(histogram.getCount()).thenReturn(1L);

		final Snapshot snapshot = mock(Snapshot.class);
		when(snapshot.getMax()).thenReturn(2L);
		when(snapshot.getMean()).thenReturn(3.0);
		when(snapshot.getMin()).thenReturn(4L);
		when(snapshot.getStdDev()).thenReturn(5.0);
		when(snapshot.getMedian()).thenReturn(6.0);
		when(snapshot.get75thPercentile()).thenReturn(7.0);
		when(snapshot.get95thPercentile()).thenReturn(8.0);
		when(snapshot.get98thPercentile()).thenReturn(9.0);
		when(snapshot.get99thPercentile()).thenReturn(10.0);
		when(snapshot.get999thPercentile()).thenReturn(11.0);

		when(histogram.getSnapshot()).thenReturn(snapshot);

		reporter.report(this.<Gauge> map(), this.<Counter> map(), this.map("histogram", histogram), this.<Meter> map(),
				this.<Timer> map());

		verify(opentsdb).send(captor.capture());

		final Set<OpenTsdbMetric> metrics = captor.getValue();
		assertEquals(11, metrics.size());

		final OpenTsdbMetric metric = metrics.iterator().next();
		assertEquals((Long) timestamp, metric.getTimestamp());

		final Map<String, Object> histMap = new HashMap<>();
		for (OpenTsdbMetric m : metrics) {
			histMap.put(m.getMetric(), m.getValue());
		}

		assertEquals(histMap.get("prefix.histogram.count"), 1L);
		assertEquals(histMap.get("prefix.histogram.max"), 2L);
		assertEquals(histMap.get("prefix.histogram.mean"), 3.0);
		assertEquals(histMap.get("prefix.histogram.min"), 4L);

		assertEquals((Double) histMap.get("prefix.histogram.stddev"), 5.0, 0.0001);
		assertEquals((Double) histMap.get("prefix.histogram.median"), 6.0, 0.0001);
		assertEquals((Double) histMap.get("prefix.histogram.p75"), 7.0, 0.0001);
		assertEquals((Double) histMap.get("prefix.histogram.p95"), 8.0, 0.0001);
		assertEquals((Double) histMap.get("prefix.histogram.p98"), 9.0, 0.0001);
		assertEquals((Double) histMap.get("prefix.histogram.p99"), 10.0, 0.0001);
		assertEquals((Double) histMap.get("prefix.histogram.p999"), 11.0, 0.0001);

	}

	@Test
	public void testReportTimers() {

		final Timer timer = mock(Timer.class);
		when(timer.getCount()).thenReturn(1L);
		when(timer.getMeanRate()).thenReturn(1.0);
		when(timer.getOneMinuteRate()).thenReturn(2.0);
		when(timer.getFiveMinuteRate()).thenReturn(3.0);
		when(timer.getFifteenMinuteRate()).thenReturn(4.0);

		final Snapshot snapshot = mock(Snapshot.class);
		when(snapshot.getMax()).thenReturn(2L);
		when(snapshot.getMin()).thenReturn(4L);
		when(snapshot.getMean()).thenReturn(3.0);
		when(snapshot.getStdDev()).thenReturn(5.0);
		when(snapshot.getMedian()).thenReturn(6.0);
		when(snapshot.get75thPercentile()).thenReturn(7.0);
		when(snapshot.get95thPercentile()).thenReturn(8.0);
		when(snapshot.get98thPercentile()).thenReturn(9.0);
		when(snapshot.get99thPercentile()).thenReturn(10.0);
		when(snapshot.get999thPercentile()).thenReturn(11.0);

		when(timer.getSnapshot()).thenReturn(snapshot);

		reporter.report(this.<Gauge> map(), this.<Counter> map(), this.<Histogram> map(), this.<Meter> map(),
				this.map("timer", timer));

		verify(opentsdb).send(captor.capture());

		final Set<OpenTsdbMetric> metrics = captor.getValue();
		assertEquals(15, metrics.size());

		final OpenTsdbMetric metric = metrics.iterator().next();
		assertEquals((Long) timestamp, metric.getTimestamp());

		final Map<String, Object> timerMap = new HashMap<>();
		for (OpenTsdbMetric m : metrics) {
			timerMap.put(m.getMetric(), m.getValue());
		}

		assertEquals(timerMap.get("prefix.timer.count"), 1L);

		// 时长是毫秒，所以在输出前需要转换成1E-6精度
		assertEquals((Double) timerMap.get("prefix.timer.max"), 2E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.mean"), 3.0E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.min"), 4E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.stddev"), 5.0E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.p75"), 7.0E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.p95"), 8.0E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.p98"), 9.0E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.p99"), 10.0E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.p999"), 11.0E-6, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.median"), 6.0E-6, 0.0001);

		// 比率转换成秒
		assertEquals((Double) timerMap.get("prefix.timer.mean_rate"), 1.0, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.m1"), 2.0, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.m5"), 3.0, 0.0001);
		assertEquals((Double) timerMap.get("prefix.timer.m15"), 4.0, 0.0001);
	}

	@Test
	public void testReportMeter() {

		final Meter meter = mock(Meter.class);
		when(meter.getCount()).thenReturn(1L);
		when(meter.getMeanRate()).thenReturn(1.0);
		when(meter.getOneMinuteRate()).thenReturn(2.0);
		when(meter.getFiveMinuteRate()).thenReturn(3.0);
		when(meter.getFifteenMinuteRate()).thenReturn(4.0);

		reporter.report(this.<Gauge> map(), this.<Counter> map(), this.<Histogram> map(), this.map("meter", meter),
				this.<Timer> map());

		verify(opentsdb).send(captor.capture());

		final Set<OpenTsdbMetric> metrics = captor.getValue();
		assertEquals(5, metrics.size());

		final OpenTsdbMetric metric = metrics.iterator().next();
		assertEquals((Long) timestamp, metric.getTimestamp());

		final Map<String, Object> meterMap = new HashMap<>();
		for (OpenTsdbMetric m : metrics) {
			meterMap.put(m.getMetric(), m.getValue());
		}

		assertEquals(meterMap.get("prefix.meter.count"), 1L);

		//convert rate to seconds,
		assertEquals((Double) meterMap.get("prefix.meter.mean_rate"), 1.0, 0.0001);
		assertEquals((Double) meterMap.get("prefix.meter.m1"), 2.0, 0.0001);
		assertEquals((Double) meterMap.get("prefix.meter.m5"), 3.0, 0.0001);
		assertEquals((Double) meterMap.get("prefix.meter.m15"), 4.0, 0.0001);
	}

	/**
	 * 测试空的Metric集合发送到OpenTSDB失败情况，因为OpenTSDB解析JSON数据时需要通过验证。
	 * 对于dropwizard的jvm.threads.deadlocks指标来说是个细节问题，因为这个指标在正常的操作条件下（无死锁）包含一个空集合
	 */
	@Test
	public void testEmptyGaugeSet() {
		final Gauge gauge = mock(Gauge.class);
		when(gauge.getValue()).thenReturn(new HashSet<String>());
		reporter.report(this.map("gauge", gauge), this.<Counter> map(), this.<Histogram> map(), this.<Meter> map(),
				this.<Timer> map());

		verify(opentsdb).send(captor.capture());

		final Set<OpenTsdbMetric> metrics = captor.getValue();
		assertEquals(0, metrics.size());
	}

	private <T> SortedMap<String, T> map() {
		return new TreeMap<String, T>();
	}

	private <T> SortedMap<String, T> map(String name, T metric) {
		final TreeMap<String, T> map = new TreeMap<>();
		map.put(name, metric);
		return map;
	}

}
