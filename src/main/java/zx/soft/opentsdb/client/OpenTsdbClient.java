package zx.soft.opentsdb.client;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.opentsdb.metric.OpenTsdbMetric;
import zx.soft.utils.log.LogbackUtil;

/**
 * 基于jersey开发OpenTSDB的REST客户端
 * <p/>
 * {@link http://opentsdb.net/docs/build/html/api_http/index.html#version-1-x-to-2-x}
 *
 * @author wanggang
 *
 */
public class OpenTsdbClient {

	private static final Logger logger = LoggerFactory.getLogger(OpenTsdbClient.class);

	// 批量处理大小
	// 设置批量大小是因为OpenTSDB中存在Metric批量发送时失败问题，见：
	// https://groups.google.com/forum/#!topic/opentsdb/U-0ak_v8qu0
	// 推荐批量大小为5～10是比较安全的，或者也可以开启分块请求（chunked request）
	public static final int DEFAULT_BATCH_SIZE_LIMIT = 0;

	// 连接超时
	public static final int CONN_TIMEOUT_DEFAULT_MS = 5000;

	// 读超时
	public static final int READ_TIMEOUT_DEFAULT_MS = 5000;

	private final WebTarget apiResource;

	// 批量大小上限
	private int batchSizeLimit = DEFAULT_BATCH_SIZE_LIMIT;

	private OpenTsdbClient(WebTarget apiResource) {
		this.apiResource = apiResource;
	}

	private OpenTsdbClient(String baseURL, Integer connectionTimeout, Integer readTimeout) {
		final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
		client.property(ClientProperties.CONNECT_TIMEOUT, connectionTimeout);
		client.property(ClientProperties.READ_TIMEOUT, readTimeout);

		this.apiResource = client.target(baseURL);
	}

	public void setBatchSizeLimit(int batchSizeLimit) {
		this.batchSizeLimit = batchSizeLimit;
	}

	/**
	 * 基于OpenTSDB服务端url初始化
	 *
	 * @param baseUrl OpenTSDB服务URL
	 * @return
	 */
	public static Builder forService(String baseUrl) {
		return new Builder(baseUrl);
	}

	/**
	 * 通过基本的WebResource创建客户端
	 *
	 * @param apiResource
	 * @return
	 */
	public static OpenTsdbClient create(WebTarget apiResource) {
		return new OpenTsdbClient(apiResource);
	}

	public static class Builder {

		private Integer connectionTimeout = CONN_TIMEOUT_DEFAULT_MS;
		private Integer readTimeout = READ_TIMEOUT_DEFAULT_MS;
		private final String baseUrl;

		public Builder(String baseUrl) {
			this.baseUrl = baseUrl;
		}

		public Builder withConnectTimeout(Integer connectionTimeout) {
			this.connectionTimeout = connectionTimeout;
			return this;
		}

		public Builder withReadTimeout(Integer readTimeout) {
			this.readTimeout = readTimeout;
			return this;
		}

		public OpenTsdbClient create() {
			return new OpenTsdbClient(baseUrl, connectionTimeout, readTimeout);
		}

	}

	/**
	 * 发送一条Metric数据到OpenTSDB中
	 *
	 * @param metric 单条数据
	 */
	public void send(OpenTsdbMetric metric) {
		send(Collections.singleton(metric));
	}

	/**
	 * 发送Metric集合数据到OpenTSDB中
	 *
	 * @param metrics 集合数据
	 */
	public void send(Set<OpenTsdbMetric> metrics) {
		if (batchSizeLimit > 0 && metrics.size() > batchSizeLimit) {
			final Set<OpenTsdbMetric> smallMetrics = new HashSet<>();
			for (final OpenTsdbMetric metric : metrics) {
				smallMetrics.add(metric);
				if (smallMetrics.size() >= batchSizeLimit) {
					sendHelper(smallMetrics);
					smallMetrics.clear();
				}
			}
			sendHelper(smallMetrics);
		} else {
			sendHelper(metrics);
		}
	}

	/**
	 * 发送帮助信息
	 *
	 * @param metrics Metric集合数据
	 */
	private void sendHelper(Set<OpenTsdbMetric> metrics) {
		/*
		 * 如果需要绑定指定的API版本，参考：http://opentsdb.net/docs/build/html/api_http/index.html#api-versioning
		 * "如果没有提供明确的版本，... 默认使用最新的版本。"
		 * 如果有问题，请回滚。
		 */
		if (!metrics.isEmpty()) {
			try {
				final Entity<?> entity = Entity.entity(metrics, MediaType.APPLICATION_JSON);
				apiResource.path("/api/put").request().post(entity);
			} catch (Exception e) {
				logger.error("Send to OpenTSDB endpoint failed, Exception: {}.", LogbackUtil.expection2Str(e));
			}
		}
	}

}
