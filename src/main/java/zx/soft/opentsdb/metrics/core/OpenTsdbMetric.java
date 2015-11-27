package zx.soft.opentsdb.metrics.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * OpenTSDB一条统计数据模型
 *
 * @author wanggang
 *
 */
public class OpenTsdbMetric {

	// 统计名称
	private String metric;

	// 统计时间
	private Long timestamp;

	// 统计值
	private Object value;

	// 统计维度数据key-value键值对
	private Map<String, String> tags = new HashMap<>();

	private OpenTsdbMetric() {
	}

	public OpenTsdbMetric(String metric, Number value, String... tags) {
		if (tags.length % 2 != 0) {
			throw new RuntimeException("tags format: k1, v1, k2, v2...");
		}
		this.metric = metric;
		this.timestamp = System.currentTimeMillis() / 1000;
		this.value = value;
		for (int i = 0; i < tags.length; i += 2) {
			this.tags.put(tags[i], tags[i + 1]);
		}
	}

	public static Builder named(String metric) {
		return new Builder(metric);
	}

	/**
	 * 命令组装
	 */
	public String serialize() {
		StringBuilder result = new StringBuilder("put ").append(metric).append(" ").append(timestamp).append(" ")
				.append(value);
		for (Entry<String, String> entry : tags.entrySet()) {
			result.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
		}
		return result.toString();
	}

	@Override
	public boolean equals(Object o) {

		if (o == this) {
			return true;
		}

		if (!(o instanceof OpenTsdbMetric)) {
			return false;
		}

		final OpenTsdbMetric rhs = (OpenTsdbMetric) o;

		return equals(metric, rhs.metric) && equals(timestamp, rhs.timestamp) && equals(value, rhs.value)
				&& equals(tags, rhs.tags);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(new Object[] { metric, timestamp, value, tags });
	}

	public static class Builder {

		private final OpenTsdbMetric openTsdbMetric;

		public Builder(String metric) {
			this.openTsdbMetric = new OpenTsdbMetric();
			openTsdbMetric.metric = metric;
		}

		public OpenTsdbMetric build() {
			return openTsdbMetric;
		}

		public Builder withValue(Object value) {
			openTsdbMetric.value = value;
			return this;
		}

		public Builder withTimestamp(Long timestamp) {
			openTsdbMetric.timestamp = timestamp;
			return this;
		}

		public Builder withTags(Map<String, String> tags) {
			if (tags != null) {
				openTsdbMetric.tags.putAll(tags);
			}
			return this;
		}

	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "->metric: " + metric + ",value: " + value + ",timestamp: "
				+ timestamp + ",tags: " + tags;
	}

	public String getMetric() {
		return metric;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public Object getValue() {
		return value;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	private boolean equals(Object a, Object b) {
		return (a == b) || (a != null && a.equals(b));
	}

}

/*
{
    "metric": "sys.cpu.nice",
    "timestamp": 1346846400,
    "value": 18,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}
 */