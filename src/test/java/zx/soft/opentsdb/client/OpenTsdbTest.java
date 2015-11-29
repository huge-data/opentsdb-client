package zx.soft.opentsdb.client;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import zx.soft.opentsdb.metric.OpenTsdbMetric;

@RunWith(MockitoJUnitRunner.class)
public class OpenTsdbTest {

	private OpenTsdbClient openTsdb;

	@Mock
	private WebTarget apiResource;

	@Mock
	Invocation.Builder mockBuilder;

	@Before
	public void setUp() {
		openTsdb = OpenTsdbClient.create(apiResource);
		openTsdb.setBatchSizeLimit(10);
	}

	@Test
	public void testSend() {
		when(apiResource.path("/api/put")).thenReturn(apiResource);
		when(apiResource.request()).thenReturn(mockBuilder);
		when(mockBuilder.post((Entity<?>) anyObject())).thenReturn(mock(Response.class));
		openTsdb.send(OpenTsdbMetric.named("foo").build());
		verify(mockBuilder).post((Entity<?>) anyObject());
	}

	@Test
	public void testSendMultiple() {
		when(apiResource.path("/api/put")).thenReturn(apiResource);
		when(apiResource.request()).thenReturn(mockBuilder);
		when(mockBuilder.post((Entity<?>) anyObject())).thenReturn(mock(Response.class));

		Set<OpenTsdbMetric> metrics = new HashSet<>(Arrays.asList(OpenTsdbMetric.named("foo").build()));
		openTsdb.send(metrics);
		verify(mockBuilder, times(1)).post((Entity<?>) anyObject());

		// 分两次请求
		for (int i = 1; i < 20; i++) {
			metrics.add(OpenTsdbMetric.named("foo" + i).build());
		}
		openTsdb.send(metrics);
		verify(mockBuilder, times(3)).post((Entity<?>) anyObject());
	}

	@Test
	public void testBuilder() {
		assertNotNull(OpenTsdbClient.forService("foo").withReadTimeout(1).withConnectTimeout(1).create());
	}

}
