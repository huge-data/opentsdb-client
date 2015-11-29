package zx.soft.opentsdb.metric;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

public class OpenTsdbMetricTest {

	@Test
	public void testEquals() {
		OpenTsdbMetric o1 = OpenTsdbMetric.named(null).withValue(1L).withTimestamp(null).withTags(null).build();

		OpenTsdbMetric o2 = OpenTsdbMetric.named(null).withValue(1L).withTimestamp(null).withTags(null).build();

		OpenTsdbMetric o3 = OpenTsdbMetric.named(null).withValue(1L).withTimestamp(null)
				.withTags(Collections.singletonMap("foo", "bar")).build();

		assertTrue(o1.equals(o1));

		assertFalse(o1.equals(new Object()));

		assertTrue(o1.equals(o2));
		assertTrue(o2.equals(o1));
		assertFalse(o1.equals(o3));
		assertFalse(o3.equals(o1));

		assertTrue(o1.hashCode() == o2.hashCode());
		assertFalse(o3.hashCode() == o2.hashCode());

		assertNotNull(o1.toString());
	}

}
